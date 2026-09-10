-- ────────────────────────────────────────────────────────────────────────────
-- PATHFINDER — SQLite mirror of migrations/0001_pathfinder.sql
--
-- WHY THIS FILE EXISTS. 0001 is the authoritative Postgres DDL and remains the
-- target. Session P1 had no reachable Postgres (no driver installed, and the local
-- instance needs a password P1 will not supply — see docs/handbacks/P0.md § Risks 1),
-- so the loop had to be able to run and be *verified* against something. This is that
-- something: the SAME table names, the SAME column names, and the SAME laws, ported
-- to SQLite so the engine could be executed and tested end-to-end today.
--
-- It is a mirror, not a fork. `repository.py` writes identical SQL against either.
-- Differences forced by the dialect, all listed here so none of them is a surprise:
--
--   * `text[]` columns become JSON arrays in TEXT. `cardinality(x) >= 1` becomes a
--     `json_array_length` CHECK.
--   * `jsonb` becomes TEXT holding JSON; the `?|` "has no target key" CHECK becomes a
--     LIKE-based CHECK over the serialised document.
--   * Postgres regex CHECKs (`computed_by !~* '(claude|gpt|...)'`, the no-bare-numeral
--     rule) have no SQLite equivalent in a CHECK, so they are enforced by BEFORE
--     INSERT triggers calling the application-defined functions `pf_names_a_model()`
--     and `pf_has_bare_numeral()`. `repository.py` registers those on every
--     connection; if it did not, inserts would fail with "no such function", which is
--     the correct direction to fail in.
--   * `deny_mutation()` becomes one BEFORE UPDATE and one BEFORE DELETE trigger per
--     history table, each raising ABORT.
--   * `bigserial` becomes INTEGER PRIMARY KEY AUTOINCREMENT.
--   * pgvector recall is Postgres-only; `recall_embeddings` here stores the pointer
--     columns and a JSON embedding so recall is exercised, with a linear scan instead
--     of HNSW. It is still never a source of truth.
-- ────────────────────────────────────────────────────────────────────────────

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS constitution_versions (
  version           TEXT PRIMARY KEY,
  previous_version  TEXT REFERENCES constitution_versions(version),
  effective_from    TEXT NOT NULL,
  document          TEXT NOT NULL,
  authored_by       TEXT NOT NULL DEFAULT 'human',
  approved_by       TEXT NOT NULL,
  prev_hash         TEXT,
  row_hash          TEXT NOT NULL,
  created_at        TEXT NOT NULL,
  CONSTRAINT constitution_is_human_only CHECK (authored_by = 'human')
);

CREATE TABLE IF NOT EXISTS llm_calls (
  llm_call_id       TEXT PRIMARY KEY,
  at                TEXT NOT NULL,
  budget_day_ist    TEXT NOT NULL,
  job               TEXT NOT NULL CHECK (job IN ('hypothesis','critique','decide_next','narrate','classify','hard_research')),
  model             TEXT NOT NULL,
  -- P1 addition over 0001: which provider served the call. A recorded replay and a
  -- live call are different facts about the world and the record says which.
  provider          TEXT NOT NULL CHECK (provider IN ('live:anthropic','recorded','none')),
  prompt_version    TEXT NOT NULL,
  schema_id         TEXT NOT NULL,
  batched           INTEGER NOT NULL DEFAULT 0,
  experiment_id     TEXT,
  cycle_id          TEXT,
  input_tokens      INTEGER NOT NULL CHECK (input_tokens >= 0),
  output_tokens     INTEGER NOT NULL CHECK (output_tokens >= 0),
  cache_read_input_tokens     INTEGER NOT NULL DEFAULT 0 CHECK (cache_read_input_tokens >= 0),
  cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_creation_input_tokens >= 0),
  cost_usd          REAL NOT NULL CHECK (cost_usd >= 0),
  latency_ms        INTEGER,
  ok                INTEGER NOT NULL,
  error_code        TEXT,
  CONSTRAINT llm_model_is_routed CHECK (model IN ('claude-sonnet-5','claude-haiku-4-5','claude-opus-5'))
);
CREATE INDEX IF NOT EXISTS llm_calls_by_day ON llm_calls (budget_day_ist, model);

CREATE TABLE IF NOT EXISTS hypotheses (
  hypothesis_id     TEXT PRIMARY KEY,
  question          TEXT NOT NULL,
  statement         TEXT NOT NULL,
  rationale         TEXT NOT NULL,
  proposed_by       TEXT NOT NULL CHECK (proposed_by IN ('engine','llm','human')),
  model             TEXT,
  prompt_version    TEXT,
  llm_call_id       TEXT REFERENCES llm_calls(llm_call_id),
  novelty_recall_ids TEXT NOT NULL DEFAULT '[]',
  constitution_version TEXT NOT NULL REFERENCES constitution_versions(version),
  created_at        TEXT NOT NULL,
  CONSTRAINT hypothesis_model_iff_llm CHECK ((proposed_by = 'llm') = (model IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS experiments (
  experiment_id     TEXT PRIMARY KEY,
  hypothesis_id     TEXT NOT NULL REFERENCES hypotheses(hypothesis_id),
  opened_at         TEXT NOT NULL,
  universe          TEXT NOT NULL,
  direction         TEXT NOT NULL CHECK (direction IN ('long','short')),
  horizon_sessions  INTEGER NOT NULL CHECK (horizon_sessions >= 1),
  cost_convention   TEXT NOT NULL,
  data_source       TEXT NOT NULL,
  constitution_version TEXT NOT NULL REFERENCES constitution_versions(version),
  created_by        TEXT NOT NULL CHECK (created_by IN ('engine','llm','human')),
  created_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
  decision_id       TEXT PRIMARY KEY,
  at                TEXT NOT NULL,
  kind              TEXT NOT NULL,
  experiment_id     TEXT REFERENCES experiments(experiment_id),
  trigger_id        TEXT,
  question          TEXT NOT NULL,
  decision          TEXT NOT NULL,
  rationale_story_line_id TEXT,
  evidence_ids      TEXT NOT NULL DEFAULT '[]',
  decided_by        TEXT NOT NULL CHECK (decided_by IN ('engine','llm','human')),
  model             TEXT,
  llm_call_id       TEXT REFERENCES llm_calls(llm_call_id),
  approved_by       TEXT,
  constitution_version TEXT NOT NULL REFERENCES constitution_versions(version),
  prev_hash         TEXT,
  row_hash          TEXT NOT NULL,
  CONSTRAINT decision_model_iff_llm CHECK ((decided_by = 'llm') = (model IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS decisions_by_time ON decisions (at DESC);

CREATE TABLE IF NOT EXISTS experiment_state (
  state_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  experiment_id     TEXT NOT NULL REFERENCES experiments(experiment_id),
  status            TEXT NOT NULL CHECK (status IN ('queued','testing','validating','promising','promoted','died')),
  at                TEXT NOT NULL,
  decision_id       TEXT REFERENCES decisions(decision_id),
  reason            TEXT NOT NULL,
  prev_hash         TEXT,
  row_hash          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS experiment_state_by_exp ON experiment_state (experiment_id, at DESC);

CREATE TABLE IF NOT EXISTS strategy_versions (
  strategy_version  TEXT PRIMARY KEY,
  experiment_id     TEXT NOT NULL REFERENCES experiments(experiment_id),
  version_label     TEXT NOT NULL,
  previous_version  TEXT REFERENCES strategy_versions(strategy_version),
  rulebook          TEXT NOT NULL,
  created_by        TEXT NOT NULL CHECK (created_by IN ('engine','llm','human')),
  approved_by       TEXT,
  validation        TEXT,
  active_from       TEXT,
  retired_at        TEXT,
  created_at        TEXT NOT NULL,
  CONSTRAINT rulebook_has_an_invalidation
    CHECK (rulebook LIKE '%"invalidation"%' AND rulebook LIKE '%"entry"%' AND rulebook LIKE '%"exit"%'),
  CONSTRAINT rulebook_has_no_target_price
    CHECK (rulebook NOT LIKE '%"target"%' AND rulebook NOT LIKE '%"target_price"%'
           AND rulebook NOT LIKE '%"price_target"%' AND rulebook NOT LIKE '%"expected_return"%'),
  CONSTRAINT replacement_requires_validation
    CHECK (previous_version IS NULL OR validation IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS parameters (
  parameter_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  strategy_version  TEXT NOT NULL REFERENCES strategy_versions(strategy_version),
  name              TEXT NOT NULL,
  value_num         REAL,
  value_text        TEXT,
  unit              TEXT,
  approved_min      REAL,
  approved_max      REAL,
  set_by            TEXT NOT NULL CHECK (set_by IN ('engine','llm','human')),
  at                TEXT NOT NULL,
  CONSTRAINT parameter_has_a_value CHECK (value_num IS NOT NULL OR value_text IS NOT NULL),
  CONSTRAINT parameter_within_approved_range CHECK (
    value_num IS NULL
    OR ((approved_min IS NULL OR value_num >= approved_min)
        AND (approved_max IS NULL OR value_num <= approved_max))
  )
);
CREATE INDEX IF NOT EXISTS parameters_by_version ON parameters (strategy_version, name);

CREATE TABLE IF NOT EXISTS facts (
  fact_id           TEXT PRIMARY KEY,
  experiment_id     TEXT REFERENCES experiments(experiment_id),
  label             TEXT NOT NULL,
  value_num         REAL,
  value_text        TEXT,
  unit              TEXT NOT NULL,
  n                 INTEGER CHECK (n IS NULL OR n >= 0),
  data_source       TEXT NOT NULL,
  range_start       TEXT NOT NULL,
  range_end         TEXT NOT NULL,
  as_of             TEXT NOT NULL,
  cost_convention   TEXT NOT NULL,
  computed_by       TEXT NOT NULL,
  computed_at       TEXT NOT NULL,
  universe          TEXT,
  note              TEXT,
  CONSTRAINT fact_has_a_value CHECK (value_num IS NOT NULL OR value_text IS NOT NULL),
  CONSTRAINT fact_window_is_ordered CHECK (range_start <= range_end),
  CONSTRAINT fact_no_look_ahead CHECK (range_end <= as_of)
);
CREATE INDEX IF NOT EXISTS facts_by_experiment ON facts (experiment_id, as_of DESC);

CREATE TABLE IF NOT EXISTS outcomes (
  outcome_id        TEXT PRIMARY KEY,
  experiment_id     TEXT NOT NULL REFERENCES experiments(experiment_id),
  strategy_version  TEXT REFERENCES strategy_versions(strategy_version),
  basis             TEXT NOT NULL CHECK (basis IN ('historical_replay','virtual_book')),
  label             TEXT NOT NULL,
  expectancy_pct_per_trade             REAL NOT NULL,
  expectancy_2x_slippage_pct_per_trade REAL NOT NULL,
  total_return_pct  REAL,
  max_drawdown_pct  REAL NOT NULL CHECK (max_drawdown_pct >= 0),
  current_drawdown_pct REAL NOT NULL CHECK (current_drawdown_pct >= 0),
  win_rate_pct      REAL CHECK (win_rate_pct IS NULL OR (win_rate_pct BETWEEN 0 AND 100)),
  avg_win_pct       REAL,
  avg_loss_pct      REAL CHECK (avg_loss_pct IS NULL OR avg_loss_pct <= 0),
  payoff_ratio      REAL,
  n                 INTEGER NOT NULL CHECK (n >= 0),
  occurrences       INTEGER CHECK (occurrences IS NULL OR occurrences >= n),
  data_source       TEXT NOT NULL,
  range_start       TEXT NOT NULL,
  range_end         TEXT NOT NULL,
  as_of             TEXT NOT NULL,
  cost_convention   TEXT NOT NULL,
  computed_by       TEXT NOT NULL,
  computed_at       TEXT NOT NULL,
  prev_hash         TEXT,
  row_hash          TEXT NOT NULL,
  CONSTRAINT outcome_no_look_ahead CHECK (range_end <= as_of),
  CONSTRAINT outcome_label_matches_basis CHECK (
    (basis = 'historical_replay' AND label LIKE 'Simulated%')
    OR (basis = 'virtual_book' AND label LIKE 'Virtual money%')
  )
);
CREATE INDEX IF NOT EXISTS outcomes_by_experiment ON outcomes (experiment_id, as_of DESC);

CREATE TABLE IF NOT EXISTS trades (
  trade_id          TEXT PRIMARY KEY,
  experiment_id     TEXT NOT NULL REFERENCES experiments(experiment_id),
  strategy_version  TEXT REFERENCES strategy_versions(strategy_version),
  symbol            TEXT NOT NULL,
  direction         TEXT NOT NULL CHECK (direction IN ('long','short')),
  signal_date       TEXT NOT NULL,
  entry_date        TEXT NOT NULL,
  entry_price       REAL NOT NULL CHECK (entry_price > 0),
  exit_date         TEXT,
  exit_price        REAL CHECK (exit_price IS NULL OR exit_price > 0),
  exit_reason       TEXT,
  holding_sessions  INTEGER CHECK (holding_sessions IS NULL OR holding_sessions >= 0),
  pnl_pct_gross     REAL,
  pnl_pct_net       REAL,
  costs_pct         REAL CHECK (costs_pct IS NULL OR costs_pct >= 0),
  slippage_bps      REAL NOT NULL CHECK (slippage_bps >= 0),
  mfe_pct           REAL,
  mae_pct           REAL,
  as_of             TEXT NOT NULL,
  computed_by       TEXT NOT NULL,
  created_at        TEXT NOT NULL,
  CONSTRAINT entry_is_after_the_signal_bar CHECK (entry_date > signal_date),
  CONSTRAINT exit_after_entry CHECK (exit_date IS NULL OR exit_date >= entry_date),
  CONSTRAINT closed_trade_is_costed
    CHECK (exit_date IS NULL OR (pnl_pct_net IS NOT NULL AND costs_pct IS NOT NULL))
);
-- Ascending by net P&L: the ledger is read LOSERS FIRST, so the index serves the default order.
CREATE INDEX IF NOT EXISTS trades_by_experiment ON trades (experiment_id, pnl_pct_net ASC);

CREATE TABLE IF NOT EXISTS evidence (
  evidence_id       TEXT PRIMARY KEY,
  experiment_id     TEXT NOT NULL REFERENCES experiments(experiment_id),
  kind              TEXT NOT NULL CHECK (kind IN ('historical_replay','forward_virtual','regime_split','cost_sensitivity','placebo','novelty_check')),
  title             TEXT NOT NULL,
  label             TEXT NOT NULL,
  outcome_id        TEXT REFERENCES outcomes(outcome_id),
  fact_ids          TEXT NOT NULL DEFAULT '[]',
  data_source       TEXT NOT NULL,
  range_start       TEXT NOT NULL,
  range_end         TEXT NOT NULL,
  as_of             TEXT NOT NULL,
  cost_convention   TEXT NOT NULL,
  computed_by       TEXT NOT NULL,
  computed_at       TEXT NOT NULL,
  interpretation_story_line_id TEXT,
  CONSTRAINT evidence_no_look_ahead CHECK (range_end <= as_of)
);

CREATE TABLE IF NOT EXISTS triggers (
  trigger_id        TEXT PRIMARY KEY,
  type              TEXT NOT NULL CHECK (type IN ('unusual_market_condition','observation_threshold','performance_deviation','scheduled_review','human_request')),
  description       TEXT NOT NULL,
  experiment_id     TEXT REFERENCES experiments(experiment_id),
  fired_at          TEXT NOT NULL,
  fired_by          TEXT NOT NULL DEFAULT 'engine',
  fact_ids          TEXT NOT NULL DEFAULT '[]',
  detail            TEXT NOT NULL DEFAULT '{}',
  CONSTRAINT trigger_fired_by_observer_or_human CHECK (fired_by IN ('engine','human'))
);
CREATE INDEX IF NOT EXISTS triggers_by_time ON triggers (fired_at DESC);

CREATE TABLE IF NOT EXISTS story_lines (
  story_line_id     TEXT PRIMARY KEY,
  experiment_id     TEXT REFERENCES experiments(experiment_id),
  cycle_id          TEXT,
  beat              TEXT NOT NULL CHECK (beat IN ('noticed','hypothesis','experiment','outcome','learning','next')),
  headline          TEXT NOT NULL,
  body              TEXT NOT NULL,
  produced_by       TEXT NOT NULL CHECK (produced_by IN ('engine','llm','human')),
  model             TEXT,
  prompt_version    TEXT,
  llm_call_id       TEXT REFERENCES llm_calls(llm_call_id),
  fact_ids          TEXT NOT NULL DEFAULT '[]',
  at                TEXT NOT NULL,
  CONSTRAINT story_model_iff_llm CHECK ((produced_by = 'llm') = (model IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS story_lines_by_experiment ON story_lines (experiment_id, at);

CREATE TABLE IF NOT EXISTS learning_events (
  learning_event_id TEXT PRIMARY KEY,
  experiment_id     TEXT REFERENCES experiments(experiment_id),
  seq               INTEGER NOT NULL CHECK (seq >= 1),
  level             TEXT NOT NULL CHECK (level IN ('L1','L2','L3','L4')),
  at                TEXT NOT NULL,
  what_changed      TEXT NOT NULL,
  why               TEXT NOT NULL,
  evidence_ids      TEXT NOT NULL,
  previous_version  TEXT,
  new_version       TEXT NOT NULL,
  outcome_before_id TEXT REFERENCES outcomes(outcome_id),
  outcome_after_id  TEXT REFERENCES outcomes(outcome_id),
  improved          INTEGER,
  decided_by        TEXT NOT NULL CHECK (decided_by IN ('engine','llm','human')),
  approved_by       TEXT,
  validation        TEXT,
  constitution_version TEXT NOT NULL REFERENCES constitution_versions(version),
  statement_story_line_id TEXT REFERENCES story_lines(story_line_id),
  prev_hash         TEXT,
  row_hash          TEXT NOT NULL,
  CONSTRAINT change_must_move_the_version
    CHECK (previous_version IS NULL OR previous_version <> new_version),
  CONSTRAINT evidence_is_mandatory CHECK (json_array_length(evidence_ids) >= 1),
  CONSTRAINT l4_is_human_only CHECK (
    level <> 'L4' OR (decided_by = 'human' AND approved_by IS NOT NULL)
  ),
  CONSTRAINT l3_requires_validation CHECK (level <> 'L3' OR validation IS NOT NULL),
  UNIQUE (experiment_id, seq)
);
CREATE INDEX IF NOT EXISTS learning_events_by_experiment ON learning_events (experiment_id, seq);

CREATE TABLE IF NOT EXISTS post_mortems (
  experiment_id     TEXT PRIMARY KEY REFERENCES experiments(experiment_id),
  died_at           TEXT NOT NULL,
  cause             TEXT NOT NULL,
  summary_story_line_id TEXT NOT NULL REFERENCES story_lines(story_line_id),
  what_we_kept      TEXT NOT NULL,
  evidence_ids      TEXT NOT NULL,
  retired_version   TEXT NOT NULL,
  decided_by        TEXT NOT NULL CHECK (decided_by IN ('engine','llm','human')),
  approved_by       TEXT,
  prev_hash         TEXT,
  row_hash          TEXT NOT NULL,
  CONSTRAINT post_mortem_keeps_something CHECK (length(trim(what_we_kept)) > 0),
  CONSTRAINT post_mortem_cites_evidence CHECK (json_array_length(evidence_ids) >= 1)
);

CREATE TABLE IF NOT EXISTS research_cycles (
  cycle_id          TEXT PRIMARY KEY,
  as_of             TEXT NOT NULL,
  stage             TEXT NOT NULL,
  trigger_id        TEXT REFERENCES triggers(trigger_id),
  active_experiment_id TEXT REFERENCES experiments(experiment_id),
  constitution_version TEXT NOT NULL REFERENCES constitution_versions(version),
  created_at        TEXT NOT NULL
);

-- ── Recall. NEVER a source of truth: every row points BACK to an authoritative row.
CREATE TABLE IF NOT EXISTS recall_embeddings (
  recall_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  kind          TEXT NOT NULL CHECK (kind IN ('hypothesis','experiment','journal','learning')),
  source_table  TEXT NOT NULL,
  source_id     TEXT NOT NULL,
  snippet       TEXT NOT NULL,
  embedding     TEXT NOT NULL,
  embed_model   TEXT NOT NULL,
  created_at    TEXT NOT NULL,
  UNIQUE (source_table, source_id, embed_model)
);

-- ── Read views the API serves from ──────────────────────────────────────────
-- Status is history, not a column: "where is it now" is derived, never overwritten.
CREATE VIEW IF NOT EXISTS experiments_current AS
SELECT e.*, s.status, s.at AS status_at, s.reason AS status_reason
FROM experiments e
JOIN experiment_state s ON s.state_id = (
  SELECT state_id FROM experiment_state st
  WHERE st.experiment_id = e.experiment_id
  ORDER BY at DESC, state_id DESC LIMIT 1
);

CREATE VIEW IF NOT EXISTS ledger_losers_first AS
SELECT * FROM trades WHERE exit_date IS NOT NULL ORDER BY experiment_id, pnl_pct_net ASC;

-- ── Append-only: nothing is ever edited in place. A correction is a new row. ──
-- (Postgres does this with one deny_mutation() trigger function; SQLite needs a
--  pair of triggers per table. Same law, more typing.)

CREATE TRIGGER IF NOT EXISTS constitution_versions_no_update BEFORE UPDATE ON constitution_versions
BEGIN SELECT RAISE(ABORT, 'constitution_versions is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS constitution_versions_no_delete BEFORE DELETE ON constitution_versions
BEGIN SELECT RAISE(ABORT, 'constitution_versions is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS hypotheses_no_update BEFORE UPDATE ON hypotheses
BEGIN SELECT RAISE(ABORT, 'hypotheses is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS hypotheses_no_delete BEFORE DELETE ON hypotheses
BEGIN SELECT RAISE(ABORT, 'hypotheses is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS experiments_no_update BEFORE UPDATE ON experiments
BEGIN SELECT RAISE(ABORT, 'experiments is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS experiments_no_delete BEFORE DELETE ON experiments
BEGIN SELECT RAISE(ABORT, 'experiments is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS experiment_state_no_update BEFORE UPDATE ON experiment_state
BEGIN SELECT RAISE(ABORT, 'experiment_state is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS experiment_state_no_delete BEFORE DELETE ON experiment_state
BEGIN SELECT RAISE(ABORT, 'experiment_state is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS strategy_versions_no_update BEFORE UPDATE ON strategy_versions
BEGIN SELECT RAISE(ABORT, 'strategy_versions is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS strategy_versions_no_delete BEFORE DELETE ON strategy_versions
BEGIN SELECT RAISE(ABORT, 'strategy_versions is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS parameters_no_update BEFORE UPDATE ON parameters
BEGIN SELECT RAISE(ABORT, 'parameters is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS parameters_no_delete BEFORE DELETE ON parameters
BEGIN SELECT RAISE(ABORT, 'parameters is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS facts_no_update BEFORE UPDATE ON facts
BEGIN SELECT RAISE(ABORT, 'facts is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS facts_no_delete BEFORE DELETE ON facts
BEGIN SELECT RAISE(ABORT, 'facts is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS outcomes_no_update BEFORE UPDATE ON outcomes
BEGIN SELECT RAISE(ABORT, 'outcomes is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS outcomes_no_delete BEFORE DELETE ON outcomes
BEGIN SELECT RAISE(ABORT, 'outcomes is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS trades_no_update BEFORE UPDATE ON trades
BEGIN SELECT RAISE(ABORT, 'trades is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS trades_no_delete BEFORE DELETE ON trades
BEGIN SELECT RAISE(ABORT, 'trades is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS evidence_no_update BEFORE UPDATE ON evidence
BEGIN SELECT RAISE(ABORT, 'evidence is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS evidence_no_delete BEFORE DELETE ON evidence
BEGIN SELECT RAISE(ABORT, 'evidence is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS triggers_no_update BEFORE UPDATE ON triggers
BEGIN SELECT RAISE(ABORT, 'triggers is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS triggers_no_delete BEFORE DELETE ON triggers
BEGIN SELECT RAISE(ABORT, 'triggers is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS llm_calls_no_update BEFORE UPDATE ON llm_calls
BEGIN SELECT RAISE(ABORT, 'llm_calls is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS llm_calls_no_delete BEFORE DELETE ON llm_calls
BEGIN SELECT RAISE(ABORT, 'llm_calls is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS story_lines_no_update BEFORE UPDATE ON story_lines
BEGIN SELECT RAISE(ABORT, 'story_lines is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS story_lines_no_delete BEFORE DELETE ON story_lines
BEGIN SELECT RAISE(ABORT, 'story_lines is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS decisions_no_update BEFORE UPDATE ON decisions
BEGIN SELECT RAISE(ABORT, 'decisions is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS decisions_no_delete BEFORE DELETE ON decisions
BEGIN SELECT RAISE(ABORT, 'decisions is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS learning_events_no_update BEFORE UPDATE ON learning_events
BEGIN SELECT RAISE(ABORT, 'learning_events is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS learning_events_no_delete BEFORE DELETE ON learning_events
BEGIN SELECT RAISE(ABORT, 'learning_events is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS post_mortems_no_update BEFORE UPDATE ON post_mortems
BEGIN SELECT RAISE(ABORT, 'post_mortems is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS post_mortems_no_delete BEFORE DELETE ON post_mortems
BEGIN SELECT RAISE(ABORT, 'post_mortems is append-only: DELETE is not permitted. History is the record.'); END;

CREATE TRIGGER IF NOT EXISTS research_cycles_no_update BEFORE UPDATE ON research_cycles
BEGIN SELECT RAISE(ABORT, 'research_cycles is append-only: UPDATE is not permitted. Record a new row.'); END;
CREATE TRIGGER IF NOT EXISTS research_cycles_no_delete BEFORE DELETE ON research_cycles
BEGIN SELECT RAISE(ABORT, 'research_cycles is append-only: DELETE is not permitted. History is the record.'); END;


-- ── The LLM never calculates — enforced in the DATABASE, not only in code. ───
-- `pf_names_a_model()` and `pf_has_bare_numeral()` are application-defined functions
-- registered by repository.py on every connection. If they are absent these inserts
-- fail with "no such function", which is the correct direction to fail in.

CREATE TRIGGER IF NOT EXISTS facts_not_computed_by_a_model BEFORE INSERT ON facts
WHEN pf_names_a_model(NEW.computed_by)
BEGIN SELECT RAISE(ABORT, 'facts.computed_by names a model: the LLM never calculates'); END;

CREATE TRIGGER IF NOT EXISTS outcomes_not_computed_by_a_model BEFORE INSERT ON outcomes
WHEN pf_names_a_model(NEW.computed_by)
BEGIN SELECT RAISE(ABORT, 'outcomes.computed_by names a model: the LLM never calculates'); END;

CREATE TRIGGER IF NOT EXISTS evidence_not_computed_by_a_model BEFORE INSERT ON evidence
WHEN pf_names_a_model(NEW.computed_by)
BEGIN SELECT RAISE(ABORT, 'evidence.computed_by names a model: the LLM never calculates'); END;

CREATE TRIGGER IF NOT EXISTS trades_not_computed_by_a_model BEFORE INSERT ON trades
WHEN pf_names_a_model(NEW.computed_by)
BEGIN SELECT RAISE(ABORT, 'trades.computed_by names a model: the LLM never calculates'); END;

-- LLM-authored prose, with {{fact:...}} tokens stripped, may not contain a digit.
CREATE TRIGGER IF NOT EXISTS story_lines_no_bare_numerals BEFORE INSERT ON story_lines
WHEN NEW.produced_by = 'llm'
 AND (pf_has_bare_numeral(NEW.headline) OR pf_has_bare_numeral(NEW.body))
BEGIN SELECT RAISE(ABORT, 'LLM-authored prose may not contain a literal numeral: reference a fact'); END;

-- A model may never wake itself, and may never author the Constitution.
CREATE TRIGGER IF NOT EXISTS triggers_never_fired_by_a_model BEFORE INSERT ON triggers
WHEN NEW.fired_by NOT IN ('engine','human')
BEGIN SELECT RAISE(ABORT, 'a trigger is fired by the deterministic observer or a human, never a model'); END;
