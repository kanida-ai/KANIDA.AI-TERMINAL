/**
 * TypeScript mirror of the Pathfinder API contract.
 *
 * SOURCE OF TRUTH: backend/pathfinder/schemas.py -> docs/openapi.yaml.
 * This file is a hand-maintained mirror; `npm run check:contract` diffs the
 * shapes below against the generated docs/openapi.yaml and fails on drift.
 *
 * The product laws are visible in the TYPES, so a screen cannot quietly drop them:
 *   - `expectancy_pct_per_trade` is REQUIRED, `win_rate_pct` is optional.
 *   - `max_drawdown_pct` + `current_drawdown_pct` are REQUIRED (no naked return).
 *   - there is NO target / projection / expected-return field. Do not add one.
 *   - `provenance` is required on every Fact and every PerformanceBlock.
 *   - `ledger_losers_first` arrives sorted worst-first. NEVER re-sort it.
 */

export type ExperimentStatus =
  | 'queued'
  | 'testing'
  | 'validating'
  | 'promising'
  | 'promoted'
  | 'died';

export type LoopStage =
  | 'observe'
  | 'hypothesize'
  | 'test'
  | 'interpret'
  | 'trade'
  | 'track'
  | 'review'
  | 'learn'
  | 'next';

/** The six beats of the loop story. The ORDER is the UX law. */
export type StoryBeat = 'noticed' | 'hypothesis' | 'experiment' | 'outcome' | 'learning' | 'next';

export type Author = 'engine' | 'llm' | 'human';
export type SampleFlag = 'ok' | 'flagged' | 'greyed' | 'unknown';
export type LearningLevel = 'L1' | 'L2' | 'L3' | 'L4';
export type Direction = 'long' | 'short';
export type Confidence = 'provisional' | 'supported' | 'strong';

export type TriggerType =
  | 'unusual_market_condition'
  | 'observation_threshold'
  | 'performance_deviation'
  | 'scheduled_review'
  | 'human_request';

export type DeathCause =
  | 'no_edge_after_costs'
  | 'edge_did_not_persist_oos'
  | 'sample_too_small'
  | 'data_quality'
  | 'regime_dependent_and_regime_gone'
  | 'superseded_by_newer_version';

export type EvidenceKind =
  | 'historical_replay'
  | 'forward_virtual'
  | 'regime_split'
  | 'cost_sensitivity'
  | 'placebo'
  | 'novelty_check';

export type Unit =
  | 'pct'
  | 'pct_per_trade'
  | 'bps'
  | 'count'
  | 'ratio'
  | 'days'
  | 'sessions'
  | 'x'
  | 'inr'
  | 'text';

export type DateRange = { start: string; end: string };

/** L-4 + L-5: where a number came from, and the as_of it was computed at. */
export type Provenance = {
  data_source: string;
  date_range: DateRange;
  as_of: string;
  cost_convention: string;
  /** the DETERMINISTIC component. A model id must never appear here. */
  computed_by: string;
  computed_at: string;
  universe?: string | null;
};

/** The ONLY place a number may originate. */
export type Fact = {
  id: string;
  label: string;
  value: number | string;
  unit: Unit;
  n?: number | null;
  sample_flag: SampleFlag;
  provenance: Provenance;
  note?: string | null;
};

export type PerformanceBlock = {
  basis: 'historical_replay' | 'virtual_book';
  /** mandatory honesty label - rendered verbatim, never paraphrased */
  label: string;
  expectancy_pct_per_trade: number;
  expectancy_2x_slippage_pct_per_trade: number;
  total_return_pct?: number | null;
  max_drawdown_pct: number;
  current_drawdown_pct: number;
  win_rate_pct?: number | null;
  avg_win_pct?: number | null;
  avg_loss_pct?: number | null;
  payoff_ratio?: number | null;
  n: number;
  occurrences?: number | null;
  sample_flag: SampleFlag;
  provenance: Provenance;
};

export type Spark = {
  basis: 'virtual_equity_pct' | 'historical_equity_pct';
  points: number[];
  date_range: DateRange;
  as_of: string;
};

export type Trigger = {
  id: string;
  type: TriggerType;
  description: string;
  fired_at: string;
  fired_by: 'engine';
  fact_refs: string[];
};

/** One beat of the loop story. If produced_by === 'llm' the prose holds NO numeral. */
export type StoryLine = {
  beat: StoryBeat;
  headline: string;
  body: string;
  produced_by: Author;
  model?: string | null;
  prompt_version?: string | null;
  at: string;
  fact_refs: string[];
};

export type VirtualTrade = {
  id: string;
  symbol: string;
  direction: Direction;
  signal_date: string;
  /** always the NEXT session after signal_date */
  entry_date: string;
  entry_price: number;
  exit_date?: string | null;
  exit_price?: number | null;
  exit_reason?: string | null;
  holding_sessions?: number | null;
  pnl_pct_gross?: number | null;
  pnl_pct_net?: number | null;
  costs_pct?: number | null;
  slippage_bps?: number | null;
  mfe_pct?: number | null;
  mae_pct?: number | null;
  as_of: string;
};

export type Evidence = {
  id: string;
  kind: EvidenceKind;
  title: string;
  label: string;
  performance?: PerformanceBlock | null;
  fact_refs: string[];
  provenance: Provenance;
  interpretation?: StoryLine | null;
};

export type ChangeLogEntry = {
  seq: number;
  level: LearningLevel;
  at: string;
  what_changed: string;
  why: string;
  evidence_refs: string[];
  previous_version?: string | null;
  new_version: string;
  performance_before?: PerformanceBlock | null;
  performance_after?: PerformanceBlock | null;
  /** null = not yet enough forward evidence to say. Render as "too early to say". */
  improved?: boolean | null;
  decided_by: Author;
  approved_by?: string | null;
  constitution_version: string;
  validation?: string | null;
};

export type PostMortem = {
  died_at: string;
  cause: DeathCause;
  summary: StoryLine;
  what_we_kept: string;
  evidence_refs: string[];
  retired_version: string;
  decided_by: Author;
  approved_by?: string | null;
};

export type ModelUsage = {
  model: string;
  calls: number;
  input_tokens: number;
  output_tokens: number;
  cache_read_input_tokens: number;
  cache_creation_input_tokens: number;
  cost_usd: number;
};

export type LlmUsage = {
  window: 'today_ist' | 'experiment_lifetime';
  by_model: ModelUsage[];
  total_cost_usd: number;
  daily_budget_usd?: number | null;
  budget_used_pct?: number | null;
  as_of: string;
};

export type ExperimentSummary = {
  id: string;
  hypothesis: string;
  status: ExperimentStatus;
  opened_at: string;
  strategy_version?: string | null;
  constitution_version: string;
  historical_return?: PerformanceBlock | null;
  virtual_return?: PerformanceBlock | null;
  occurrences?: number | null;
  n?: number | null;
  sample_flag: SampleFlag;
  spark?: Spark | null;
  as_of: string;
};

export type Rulebook = {
  universe: string;
  direction: Direction;
  entry: string;
  /** replaces "target price" - there is no target price in this product */
  invalidation: string;
  exit: string;
  horizon_sessions: number;
  sizing: string;
  cost_convention: string;
};

export type VirtualBook = {
  metrics: PerformanceBlock;
  capital_inr: number;
  /** ascending by pnl_pct_net. Render in the order received. */
  ledger_losers_first: VirtualTrade[];
  open_positions: VirtualTrade[];
};

export type ExperimentDetail = ExperimentSummary & {
  question: string;
  rationale: string;
  rulebook: Rulebook;
  stage: LoopStage;
  story: StoryLine[];
  facts: Fact[];
  evidence: Evidence[];
  virtual_book?: VirtualBook | null;
  change_log: ChangeLogEntry[];
  post_mortem?: PostMortem | null;
  triggers: Trigger[];
  llm_usage?: LlmUsage | null;
  next_review?: Trigger | null;
  disclosure: string;
};

export type LoopResponse = {
  as_of: string;
  cycle_id: string;
  stage: LoopStage;
  constitution_version: string;
  active_experiment_id?: string | null;
  trigger?: Trigger | null;
  story: StoryLine[];
  facts: Fact[];
  counts: Partial<Record<ExperimentStatus, number>>;
  llm_usage?: LlmUsage | null;
  disclosure: string;
};

export type ExperimentListResponse = {
  as_of: string;
  status_filter?: ExperimentStatus | null;
  count: number;
  items: ExperimentSummary[];
  disclosure: string;
};

export type Learning = {
  id: string;
  statement: StoryLine;
  level: LearningLevel;
  learned_at: string;
  from_experiments: string[];
  evidence_refs: string[];
  fact_refs: string[];
  confidence: Confidence;
  n?: number | null;
  sample_flag: SampleFlag;
  applied_in: string[];
};

export type NextTest = {
  id: string;
  question: string;
  why_now: StoryLine;
  triggered_by?: Trigger | null;
  planned_test: string;
  blocked_by?: string | null;
  queued_experiment_id?: string | null;
  fact_refs: string[];
};

export type LearningsResponse = {
  as_of: string;
  constitution_version: string;
  learned: Learning[];
  testing_next: NextTest[];
  facts: Fact[];
  disclosure: string;
};

export type ApiErrorBody = {
  error: { code: string; message: string; request_id?: string | null };
};
