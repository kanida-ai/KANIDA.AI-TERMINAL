-- ============================================================================
-- KANIDA.AI — migration 0001 — Pathfinder authoritative schema
--
-- Authority: docs/DATA_MODEL.md § Pathfinder.  Locked by docs/sessions/PATHFINDER.md:
--   Postgres = TRUTH (relational, authoritative, immutable/append-only history)
--   pgvector = ASSOCIATIVE RECALL ONLY (never a source of truth)
--
-- Target: PostgreSQL 15+.  `vector` extension optional (recall schema only).
-- Idempotent: safe to re-run.
--
-- Product laws enforced HERE, in the database, not only in application code:
--   * append-only — UPDATE/DELETE on every history table raises
--   * hash-chained — published records carry prev_hash/row_hash
--   * point-in-time — a trade cannot enter on or before its own signal bar
--   * L4 is human-only — a Constitution-level change with a non-human author
--     cannot be inserted
--   * L3 must state its validation before it may replace a strategy
--   * the LLM never calculates — LLM-authored prose containing a bare numeral
--     cannot be inserted
--   * expectancy + drawdown travel together — an outcome row without both is
--     not a legal row
-- ============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS pathfinder;
SET LOCAL search_path = pathfinder, public;

-- ── Enums ──────────────────────────────────────────────────────────────────

DO $$ BEGIN
  CREATE TYPE experiment_status AS ENUM
    ('queued','testing','validating','promising','promoted','died');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE learning_level AS ENUM ('L1','L2','L3','L4');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE author AS ENUM ('engine','llm','human');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE trigger_type AS ENUM
    ('unusual_market_condition','observation_threshold','performance_deviation',
     'scheduled_review','human_request');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE story_beat AS ENUM
    ('noticed','hypothesis','experiment','outcome','learning','next');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE outcome_basis AS ENUM ('historical_replay','virtual_book');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE evidence_kind AS ENUM
    ('historical_replay','forward_virtual','regime_split','cost_sensitivity',
     'placebo','novelty_check');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE death_cause AS ENUM
    ('no_edge_after_costs','edge_did_not_persist_oos','sample_too_small',
     'data_quality','regime_dependent_and_regime_gone','superseded_by_newer_version');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE llm_job AS ENUM
    ('hypothesis','critique','decide_next','narrate','classify','hard_research');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ── Append-only enforcement ────────────────────────────────────────────────
-- Every table below that records HISTORY gets this trigger. Nothing in the
-- Pathfinder record is ever edited in place; a correction is a new row.

CREATE OR REPLACE FUNCTION deny_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION
    'pathfinder.% is append-only (attempted %). Corrections are new rows, never edits.',
    TG_TABLE_NAME, TG_OP;
END $$;

-- ── Constitution (L4 — human-controlled, versioned) ────────────────────────
-- The agent learns INSIDE this. It can never write here: the CHECK forbids a
-- non-human author, and the API exposes no write path at all.

CREATE TABLE IF NOT EXISTS constitution_versions (
  version           text PRIMARY KEY,                       -- e.g. 'constitution@1.3.0'
  previous_version  text REFERENCES constitution_versions(version),
  effective_from    timestamptz NOT NULL,
  document          jsonb       NOT NULL,                   -- risk limits, honesty rules,
                                                            -- compliance boundaries, permitted
                                                            -- actions, approved param ranges
  authored_by       author      NOT NULL DEFAULT 'human',
  approved_by       text        NOT NULL,                   -- named human
  prev_hash         text,
  row_hash          text        NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT constitution_is_human_only CHECK (authored_by = 'human')
);
COMMENT ON TABLE constitution_versions IS
  'L4. Human-controlled only. The agent may learn inside the Constitution; it can never rewrite it.';

-- ── Hypotheses (what the LLM proposed) ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS hypotheses (
  hypothesis_id     text PRIMARY KEY,                       -- 'hyp_0007'
  question          text NOT NULL,                          -- the question the agent asked itself
  statement         text NOT NULL,                          -- the falsifiable claim
  rationale         text NOT NULL,
  proposed_by       author NOT NULL,
  model             text,                                   -- NULL unless proposed_by='llm'
  prompt_version    text,
  llm_call_id       text,                                   -- FK added after llm_calls exists
  novelty_recall_ids text[] NOT NULL DEFAULT '{}',          -- what the memory search returned
  constitution_version text NOT NULL REFERENCES constitution_versions(version),
  created_at        timestamptz NOT NULL,
  CONSTRAINT hypothesis_model_iff_llm
    CHECK ((proposed_by = 'llm') = (model IS NOT NULL))
);

-- ── Experiments (immutable identity + header) ──────────────────────────────
-- Status is NOT a column here: status changes are history, and history is
-- append-only. See experiment_state + the experiments_current view.

CREATE TABLE IF NOT EXISTS experiments (
  experiment_id     text PRIMARY KEY,                       -- 'exp_0007'
  hypothesis_id     text NOT NULL REFERENCES hypotheses(hypothesis_id),
  opened_at         timestamptz NOT NULL,
  universe          text NOT NULL,                          -- point-in-time universe id
  direction         text NOT NULL CHECK (direction IN ('long','short')),
  horizon_sessions  int  NOT NULL CHECK (horizon_sessions >= 1),
  cost_convention   text NOT NULL,                          -- versioned, e.g. 'costs_v3: ...'
  data_source       text NOT NULL,
  constitution_version text NOT NULL REFERENCES constitution_versions(version),
  created_by        author NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS experiment_state (
  state_id          bigserial PRIMARY KEY,
  experiment_id     text NOT NULL REFERENCES experiments(experiment_id),
  status            experiment_status NOT NULL,
  at                timestamptz NOT NULL,
  decision_id       text,                                   -- FK added after decisions exists
  reason            text NOT NULL,
  prev_hash         text,
  row_hash          text NOT NULL
);
CREATE INDEX IF NOT EXISTS experiment_state_by_exp ON experiment_state (experiment_id, at DESC);
COMMENT ON TABLE experiment_state IS
  'Append-only status transitions. An experiment never "changes status" — a new row is recorded.';

-- ── Strategy versions (L3) + parameters (L2) ───────────────────────────────

CREATE TABLE IF NOT EXISTS strategy_versions (
  strategy_version  text PRIMARY KEY,                       -- 'exp_0003:strategy@v3.0'
  experiment_id     text NOT NULL REFERENCES experiments(experiment_id),
  version_label     text NOT NULL,                          -- 'strategy@v3.0'
  previous_version  text REFERENCES strategy_versions(strategy_version),
  rulebook          jsonb NOT NULL,                         -- entry / invalidation / exit / sizing
  created_by        author NOT NULL,
  approved_by       text,
  validation        text,                                   -- how it was backtested AND forward-validated
  active_from       timestamptz,
  retired_at        timestamptz,
  created_at        timestamptz NOT NULL,
  CONSTRAINT rulebook_has_an_invalidation
    CHECK (rulebook ? 'invalidation' AND rulebook ? 'entry' AND rulebook ? 'exit'),
  -- No target prices, anywhere, ever.
  CONSTRAINT rulebook_has_no_target_price
    CHECK (NOT (rulebook ?| ARRAY['target','target_price','price_target','expected_return'])),
  -- A version may only REPLACE an incumbent if it says how it was validated.
  CONSTRAINT replacement_requires_validation
    CHECK (previous_version IS NULL OR validation IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS parameters (
  parameter_id      bigserial PRIMARY KEY,
  strategy_version  text NOT NULL REFERENCES strategy_versions(strategy_version),
  name              text NOT NULL,
  value_num         numeric,
  value_text        text,
  unit              text,
  approved_min      numeric,                                -- from the Constitution
  approved_max      numeric,
  set_by            author NOT NULL,
  at                timestamptz NOT NULL,
  CONSTRAINT parameter_has_a_value CHECK (value_num IS NOT NULL OR value_text IS NOT NULL),
  -- L2: the agent may tune a threshold ONLY inside its approved range.
  CONSTRAINT parameter_within_approved_range CHECK (
    value_num IS NULL
    OR ((approved_min IS NULL OR value_num >= approved_min)
        AND (approved_max IS NULL OR value_num <= approved_max))
  )
);
CREATE INDEX IF NOT EXISTS parameters_by_version ON parameters (strategy_version, name);

-- ── Facts: the ONLY place a number may originate ───────────────────────────

CREATE TABLE IF NOT EXISTS facts (
  fact_id           text PRIMARY KEY,                       -- 'fct_e3_hist_exp'
  experiment_id     text REFERENCES experiments(experiment_id),
  label             text NOT NULL,
  value_num         numeric,
  value_text        text,
  unit              text NOT NULL,
  n                 int CHECK (n IS NULL OR n >= 0),
  -- Provenance: every number carries n + date range + data source + cost convention.
  data_source       text NOT NULL,
  range_start       date NOT NULL,
  range_end         date NOT NULL,
  as_of             date NOT NULL,
  cost_convention   text NOT NULL,
  computed_by       text NOT NULL,                          -- DETERMINISTIC component id
  computed_at       timestamptz NOT NULL,
  universe          text,
  note              text,
  CONSTRAINT fact_has_a_value CHECK (value_num IS NOT NULL OR value_text IS NOT NULL),
  CONSTRAINT fact_window_is_ordered CHECK (range_start <= range_end),
  -- Point-in-time is law: a number may not be computed from data after its as_of.
  CONSTRAINT fact_no_look_ahead CHECK (range_end <= as_of),
  -- The LLM never calculates: computed_by must not name a model.
  CONSTRAINT fact_not_computed_by_a_model
    CHECK (computed_by !~* '(claude|gpt|gemini|sonnet|haiku|opus|llm)')
);
CREATE INDEX IF NOT EXISTS facts_by_experiment ON facts (experiment_id, as_of DESC);

-- ── Outcomes: the computed book, marked point-in-time ──────────────────────
-- Serialises to the API's PerformanceBlock. Expectancy is required; every
-- return is structurally paired with its drawdown.

CREATE TABLE IF NOT EXISTS outcomes (
  outcome_id        text PRIMARY KEY,                       -- 'out_e3_2026-09-08_virtual'
  experiment_id     text NOT NULL REFERENCES experiments(experiment_id),
  strategy_version  text REFERENCES strategy_versions(strategy_version),
  basis             outcome_basis NOT NULL,
  label             text NOT NULL,                          -- the mandatory honesty label

  expectancy_pct_per_trade              numeric NOT NULL,   -- HERO
  expectancy_2x_slippage_pct_per_trade  numeric NOT NULL,   -- the sensitivity gate
  total_return_pct  numeric,
  max_drawdown_pct  numeric NOT NULL CHECK (max_drawdown_pct >= 0),
  current_drawdown_pct numeric NOT NULL CHECK (current_drawdown_pct >= 0),
  win_rate_pct      numeric CHECK (win_rate_pct IS NULL OR win_rate_pct BETWEEN 0 AND 100),
  avg_win_pct       numeric,
  avg_loss_pct      numeric CHECK (avg_loss_pct IS NULL OR avg_loss_pct <= 0),
  n                 int NOT NULL CHECK (n >= 0),
  occurrences       int CHECK (occurrences IS NULL OR occurrences >= n),

  data_source       text NOT NULL,
  range_start       date NOT NULL,
  range_end         date NOT NULL,
  as_of             date NOT NULL,
  cost_convention   text NOT NULL,
  computed_by       text NOT NULL,
  computed_at       timestamptz NOT NULL,
  prev_hash         text,
  row_hash          text NOT NULL,
  CONSTRAINT outcome_no_look_ahead CHECK (range_end <= as_of),
  CONSTRAINT outcome_not_computed_by_a_model
    CHECK (computed_by !~* '(claude|gpt|gemini|sonnet|haiku|opus|llm)'),
  CONSTRAINT outcome_label_matches_basis CHECK (
    (basis = 'historical_replay' AND label LIKE 'Simulated%')
    OR (basis = 'virtual_book' AND label LIKE 'Virtual money%')
  )
);
CREATE INDEX IF NOT EXISTS outcomes_by_experiment ON outcomes (experiment_id, as_of DESC);

-- ── Trades: the virtual book ledger ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS trades (
  trade_id          text PRIMARY KEY,                       -- 'trd_e3_01'
  experiment_id     text NOT NULL REFERENCES experiments(experiment_id),
  strategy_version  text REFERENCES strategy_versions(strategy_version),
  symbol            text NOT NULL,
  direction         text NOT NULL CHECK (direction IN ('long','short')),
  signal_date       date NOT NULL,
  entry_date        date NOT NULL,
  entry_price       numeric NOT NULL CHECK (entry_price > 0),
  exit_date         date,
  exit_price        numeric CHECK (exit_price IS NULL OR exit_price > 0),
  exit_reason       text,
  holding_sessions  int CHECK (holding_sessions IS NULL OR holding_sessions >= 0),
  pnl_pct_gross     numeric,
  pnl_pct_net       numeric,
  costs_pct         numeric CHECK (costs_pct IS NULL OR costs_pct >= 0),
  slippage_bps      numeric NOT NULL CHECK (slippage_bps >= 0),
  mfe_pct           numeric,
  mae_pct           numeric,
  as_of             date NOT NULL,
  computed_by       text NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  -- Entry = NEXT OPEN after the signal bar. Not the signal bar itself. Ever.
  CONSTRAINT entry_is_after_the_signal_bar CHECK (entry_date > signal_date),
  CONSTRAINT exit_after_entry CHECK (exit_date IS NULL OR exit_date >= entry_date),
  -- Costs are charged on every simulated trade, not just the winners.
  CONSTRAINT closed_trade_is_costed
    CHECK (exit_date IS NULL OR (pnl_pct_net IS NOT NULL AND costs_pct IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS trades_by_experiment ON trades (experiment_id, pnl_pct_net ASC NULLS LAST);
COMMENT ON INDEX trades_by_experiment IS
  'Ascending by net P&L: the ledger is read LOSERS FIRST, so the index serves the default order.';

-- ── Evidence bundles ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS evidence (
  evidence_id       text PRIMARY KEY,                       -- 'evd_e3_volsplit'
  experiment_id     text NOT NULL REFERENCES experiments(experiment_id),
  kind              evidence_kind NOT NULL,
  title             text NOT NULL,
  label             text NOT NULL,                          -- honesty label
  outcome_id        text REFERENCES outcomes(outcome_id),
  fact_ids          text[] NOT NULL DEFAULT '{}',
  data_source       text NOT NULL,
  range_start       date NOT NULL,
  range_end         date NOT NULL,
  as_of             date NOT NULL,
  cost_convention   text NOT NULL,
  computed_by       text NOT NULL,
  computed_at       timestamptz NOT NULL,
  interpretation_story_line_id text,                        -- FK added after story_lines exists
  CONSTRAINT evidence_no_look_ahead CHECK (range_end <= as_of),
  CONSTRAINT evidence_not_computed_by_a_model
    CHECK (computed_by !~* '(claude|gpt|gemini|sonnet|haiku|opus|llm)')
);

-- ── Triggers: what wakes the LLM (autonomy = intelligent activation) ───────

CREATE TABLE IF NOT EXISTS triggers (
  trigger_id        text PRIMARY KEY,                       -- 'trg_e3_obs30'
  type              trigger_type NOT NULL,
  description       text NOT NULL,
  experiment_id     text REFERENCES experiments(experiment_id),
  fired_at          timestamptz NOT NULL,
  fired_by          author NOT NULL DEFAULT 'engine',
  fact_ids          text[] NOT NULL DEFAULT '{}',
  detail            jsonb NOT NULL DEFAULT '{}'::jsonb,
  -- A trigger is fired by the deterministic observer (or a human), never by the
  -- model it is about to wake, and never by a clock alone.
  CONSTRAINT trigger_fired_by_observer_or_human CHECK (fired_by IN ('engine','human'))
);
CREATE INDEX IF NOT EXISTS triggers_by_time ON triggers (fired_at DESC);

-- ── LLM calls: real token metering from day 1 ──────────────────────────────

CREATE TABLE IF NOT EXISTS llm_calls (
  llm_call_id       text PRIMARY KEY,
  at                timestamptz NOT NULL,
  budget_day_ist    date NOT NULL,                          -- the day the cap is enforced against
  job               llm_job NOT NULL,
  model             text NOT NULL,
  prompt_version    text NOT NULL,
  schema_id         text NOT NULL,                          -- structured-output schema used
  batched           boolean NOT NULL DEFAULT false,
  experiment_id     text REFERENCES experiments(experiment_id),
  cycle_id          text,
  input_tokens      int NOT NULL CHECK (input_tokens >= 0),
  output_tokens     int NOT NULL CHECK (output_tokens >= 0),
  cache_read_input_tokens     int NOT NULL DEFAULT 0 CHECK (cache_read_input_tokens >= 0),
  cache_creation_input_tokens int NOT NULL DEFAULT 0 CHECK (cache_creation_input_tokens >= 0),
  cost_usd          numeric NOT NULL CHECK (cost_usd >= 0),
  latency_ms        int,
  ok                boolean NOT NULL,
  error_code        text,
  CONSTRAINT llm_model_is_routed CHECK (
    model IN ('claude-sonnet-5','claude-haiku-4-5','claude-opus-5')
  )
);
CREATE INDEX IF NOT EXISTS llm_calls_by_day ON llm_calls (budget_day_ist, model);
COMMENT ON CONSTRAINT llm_model_is_routed ON llm_calls IS
  'The locked job->model routing. Changing this set is a Constitution (L4) decision.';

-- ── Story lines: the narrative, with the LLM-never-calculates law in the DB ─

CREATE TABLE IF NOT EXISTS story_lines (
  story_line_id     text PRIMARY KEY,
  experiment_id     text REFERENCES experiments(experiment_id),
  cycle_id          text,
  beat              story_beat NOT NULL,
  headline          text NOT NULL,
  body              text NOT NULL,
  produced_by       author NOT NULL,
  model             text,
  prompt_version    text,
  llm_call_id       text REFERENCES llm_calls(llm_call_id),
  fact_ids          text[] NOT NULL DEFAULT '{}',
  at                timestamptz NOT NULL,
  CONSTRAINT story_model_iff_llm CHECK ((produced_by = 'llm') = (model IS NOT NULL)),
  -- THE CORE PRINCIPLE, enforced by the database: LLM-authored prose may not
  -- contain a literal numeral. Numbers are referenced as {{fact:fct_...}} and
  -- resolved from the facts table at render time.
  CONSTRAINT llm_prose_has_no_bare_numerals CHECK (
    produced_by <> 'llm'
    OR (
      regexp_replace(headline, '\{\{(fact|exp|evd|ver):[A-Za-z0-9_.\-]+\}\}', '', 'g') !~ '[0-9]'
      AND regexp_replace(body,  '\{\{(fact|exp|evd|ver):[A-Za-z0-9_.\-]+\}\}', '', 'g') !~ '[0-9]'
    )
  )
);
CREATE INDEX IF NOT EXISTS story_lines_by_experiment ON story_lines (experiment_id, at);

ALTER TABLE evidence
  DROP CONSTRAINT IF EXISTS evidence_interpretation_fk,
  ADD  CONSTRAINT evidence_interpretation_fk
       FOREIGN KEY (interpretation_story_line_id) REFERENCES story_lines(story_line_id);

ALTER TABLE hypotheses
  DROP CONSTRAINT IF EXISTS hypotheses_llm_call_fk,
  ADD  CONSTRAINT hypotheses_llm_call_fk
       FOREIGN KEY (llm_call_id) REFERENCES llm_calls(llm_call_id);

-- ── Decisions: the audit of autonomy ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS decisions (
  decision_id       text PRIMARY KEY,
  at                timestamptz NOT NULL,
  kind              text NOT NULL,      -- open_experiment | allocate_capital | tune_parameter |
                                        -- cut_version | promote | kill | queue_next | abstain
  experiment_id     text REFERENCES experiments(experiment_id),
  trigger_id        text REFERENCES triggers(trigger_id),
  question          text NOT NULL,
  decision          text NOT NULL,
  rationale_story_line_id text REFERENCES story_lines(story_line_id),
  evidence_ids      text[] NOT NULL DEFAULT '{}',
  decided_by        author NOT NULL,
  model             text,
  llm_call_id       text REFERENCES llm_calls(llm_call_id),
  approved_by       text,
  constitution_version text NOT NULL REFERENCES constitution_versions(version),
  prev_hash         text,
  row_hash          text NOT NULL,
  CONSTRAINT decision_model_iff_llm CHECK ((decided_by = 'llm') = (model IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS decisions_by_time ON decisions (at DESC);

ALTER TABLE experiment_state
  DROP CONSTRAINT IF EXISTS experiment_state_decision_fk,
  ADD  CONSTRAINT experiment_state_decision_fk
       FOREIGN KEY (decision_id) REFERENCES decisions(decision_id);

-- ── Learning events: the L1–L4 change-log (customer-facing) ────────────────

CREATE TABLE IF NOT EXISTS learning_events (
  learning_event_id text PRIMARY KEY,
  experiment_id     text REFERENCES experiments(experiment_id),
  seq               int NOT NULL CHECK (seq >= 1),
  level             learning_level NOT NULL,
  at                timestamptz NOT NULL,
  what_changed      text NOT NULL,
  why               text NOT NULL,
  evidence_ids      text[] NOT NULL,
  previous_version  text,
  new_version       text NOT NULL,
  outcome_before_id text REFERENCES outcomes(outcome_id),
  outcome_after_id  text REFERENCES outcomes(outcome_id),
  improved          boolean,                                -- NULL = not yet enough evidence
  decided_by        author NOT NULL,
  approved_by       text,
  validation        text,
  constitution_version text NOT NULL REFERENCES constitution_versions(version),
  statement_story_line_id text REFERENCES story_lines(story_line_id),
  prev_hash         text,
  row_hash          text NOT NULL,
  CONSTRAINT change_must_move_the_version
    CHECK (previous_version IS NULL OR previous_version <> new_version),
  CONSTRAINT evidence_is_mandatory CHECK (cardinality(evidence_ids) >= 1),
  -- L4: the Constitution is human-controlled. The agent cannot write one.
  CONSTRAINT l4_is_human_only CHECK (
    level <> 'L4' OR (decided_by = 'human' AND approved_by IS NOT NULL)
  ),
  -- L3: a new strategy version must state how it was backtested AND
  -- forward-validated before it was allowed to replace the incumbent.
  CONSTRAINT l3_requires_validation CHECK (level <> 'L3' OR validation IS NOT NULL),
  UNIQUE (experiment_id, seq)
);
CREATE INDEX IF NOT EXISTS learning_events_by_experiment ON learning_events (experiment_id, seq);

-- ── Post-mortems: mandatory on death, published ────────────────────────────

CREATE TABLE IF NOT EXISTS post_mortems (
  experiment_id     text PRIMARY KEY REFERENCES experiments(experiment_id),
  died_at           timestamptz NOT NULL,
  cause             death_cause NOT NULL,
  summary_story_line_id text NOT NULL REFERENCES story_lines(story_line_id),
  what_we_kept      text NOT NULL,
  evidence_ids      text[] NOT NULL,
  retired_version   text NOT NULL,
  decided_by        author NOT NULL,
  approved_by       text,
  prev_hash         text,
  row_hash          text NOT NULL,
  CONSTRAINT post_mortem_keeps_something CHECK (length(btrim(what_we_kept)) > 0),
  CONSTRAINT post_mortem_cites_evidence CHECK (cardinality(evidence_ids) >= 1)
);

-- ── Research cycles (one row per loop turn) ────────────────────────────────

CREATE TABLE IF NOT EXISTS research_cycles (
  cycle_id          text PRIMARY KEY,                       -- 'cyc_2026_09_08_01'
  as_of             timestamptz NOT NULL,
  stage             text NOT NULL,
  trigger_id        text REFERENCES triggers(trigger_id),
  active_experiment_id text REFERENCES experiments(experiment_id),
  constitution_version text NOT NULL REFERENCES constitution_versions(version),
  created_at        timestamptz NOT NULL DEFAULT now()
);

-- ── Append-only triggers on every history table ────────────────────────────

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'constitution_versions','hypotheses','experiments','experiment_state',
    'strategy_versions','parameters','facts','outcomes','trades','evidence',
    'triggers','llm_calls','story_lines','decisions','learning_events',
    'post_mortems','research_cycles'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON pathfinder.%I', t || '_append_only', t);
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON pathfinder.%I '
      'FOR EACH ROW EXECUTE FUNCTION pathfinder.deny_mutation()',
      t || '_append_only', t
    );
  END LOOP;
END $$;

-- ── Read views the API serves from ─────────────────────────────────────────

CREATE OR REPLACE VIEW experiments_current AS
SELECT e.*,
       s.status,
       s.at  AS status_at,
       s.reason AS status_reason
FROM experiments e
JOIN LATERAL (
  SELECT status, at, reason
  FROM experiment_state st
  WHERE st.experiment_id = e.experiment_id
  ORDER BY at DESC, state_id DESC
  LIMIT 1
) s ON true;
COMMENT ON VIEW experiments_current IS
  'Current status derived from the append-only transition log. Never store status on experiments.';

CREATE OR REPLACE VIEW ledger_losers_first AS
SELECT *
FROM trades
WHERE exit_date IS NOT NULL
ORDER BY experiment_id, pnl_pct_net ASC;
COMMENT ON VIEW ledger_losers_first IS
  'The ONLY ordering the product ships for a closed-trade ledger. Losers first, always.';

-- ── pgvector: ASSOCIATIVE RECALL ONLY — never a source of truth ────────────
-- Answers "have I explored something similar?" and "what past work resembles
-- this?". A row here is a POINTER back into the authoritative tables above.
-- Nothing is ever read out of here as evidence.

DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS vector;
EXCEPTION WHEN insufficient_privilege OR undefined_file THEN
  RAISE NOTICE 'pgvector not installed — recall schema skipped (truth schema is unaffected).';
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
    CREATE TABLE IF NOT EXISTS pathfinder.recall_embeddings (
      recall_id     bigserial PRIMARY KEY,
      kind          text NOT NULL CHECK (kind IN ('hypothesis','experiment','journal','learning')),
      source_table  text NOT NULL,        -- the authoritative table this points BACK to
      source_id     text NOT NULL,
      snippet       text NOT NULL,
      embedding     vector(1024) NOT NULL,
      embed_model   text NOT NULL,
      created_at    timestamptz NOT NULL DEFAULT now(),
      UNIQUE (source_table, source_id, embed_model)
    );
    COMMENT ON TABLE pathfinder.recall_embeddings IS
      'ASSOCIATIVE RECALL ONLY. Never a source of truth. Every row points back to an '
      'authoritative row; evidence is read from there, never from here.';
    CREATE INDEX IF NOT EXISTS recall_embeddings_hnsw
      ON pathfinder.recall_embeddings USING hnsw (embedding vector_cosine_ops);
  END IF;
END $$;

COMMIT;
