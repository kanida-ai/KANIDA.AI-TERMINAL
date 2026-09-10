/**
 * TypeScript mirror of the Pathfinder research contract (S1 feed + S2 experiments).
 *
 * SOURCE OF TRUTH: backend/pathfinder/schemas.py -> docs/openapi.yaml.
 * This file is a hand-maintained mirror; `npm run test:contract` runs against the
 * LIVE API and fails on semantic drift (a missing provenance field, a numeral in
 * a narrative, a card below the threshold, a mislabelled backfill).
 *
 * The product laws are visible in the TYPES, so a screen cannot quietly drop them:
 *   - every `Fact` carries `provenance` (n + window + source + cost convention + level).
 *   - `Narrative` prose is digit-free; every number is a `{{fact:…}}` reference.
 *   - a `Finding` carries its `grading_rule` (frozen at publication) and `grading` state.
 *   - `backfilled` + `record_label` ride on the edition, on every card, on every grade
 *     and on the scoreboard. The app renders them; it never derives or hides them.
 *   - there is NO entry / target / stop / execution field anywhere. Do not add one.
 *   - an `ExperimentCard` has no field that could hold a constituent list.
 */

// ── Shared value objects ─────────────────────────────────────────────────────

export type Author = 'engine' | 'llm' | 'human';

/** n >= 50 ok · 20–49 flagged · < 20 greyed · none unknown · parameter/observation not_applicable */
export type SampleFlag = 'ok' | 'flagged' | 'greyed' | 'unknown' | 'not_applicable';

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

/** Where a finding's evidence comes from (spec addendum 7). */
export type EvidenceLevel = 'same_stock' | 'peer_group' | 'sector' | 'whole_market';

export type LearningLevel = 'L1' | 'L2' | 'L3' | 'L4';
export type Direction = 'long' | 'short';

/** Where a number came from, and the as_of it was computed at. */
export type Provenance = {
  data_source: string;
  date_range: DateRange;
  as_of: string;
  cost_convention: string;
  /** the DETERMINISTIC component. A model id must never appear here. */
  computed_by: string;
  computed_at: string;
  universe?: string | null;
  level?: EvidenceLevel | null;
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

/** A P0/P1 performance block; still referenced by the S2 change-log entries. */
export type PerformanceBlock = {
  basis: 'historical_replay' | 'virtual_book';
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

/** A P0-shaped story line; the S2 post-mortem summary still uses it. */
export type StoryLine = {
  beat: string;
  headline: string;
  body: string;
  produced_by: Author;
  model?: string | null;
  prompt_version?: string | null;
  at: string;
  fact_refs: string[];
};

export type DeathCause =
  | 'no_edge_after_costs'
  | 'not_implementable_under_cost_convention'
  | 'not_distinguishable_from_chance'
  | 'edge_did_not_persist_oos'
  | 'sample_too_small'
  | 'data_quality'
  | 'regime_dependent_and_regime_gone'
  | 'superseded_by_newer_version'
  | 'revisions_exhausted'
  | 'rule_stopped_firing';

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

// ── S1: the clarity-first feed ───────────────────────────────────────────────

export const BACKFILL_LABEL = 'simulated backfill — generated after the fact; not a forward track record';
export const FORWARD_LABEL = 'forward record — generated on its own session date, before the outcome';

export type Decision =
  | 'virtual_long'
  | 'virtual_short'
  | 'watch'
  | 'no_trade'
  | 'new_experiment'
  | 'continue'
  | 'reject';

export type SubjectKind = 'stock' | 'sector' | 'pair' | 'market';
export type Tier = 'what_matters_now' | 'discovery';

export type GradingKind =
  | 'directional_call'
  | 'no_trade_call'
  | 'theme_call'
  | 'theme_watch'
  | 'rotation_reject'
  | 'anomaly_move'
  | 'pair_convergence'
  | 'pair_watch';

export type Verdict = 'right' | 'wrong' | 'inconclusive' | 'void';
export type GradingStatus = 'pending' | 'graded' | 'continued' | 'void';

/** On EVERY card: level, n, period, regime, comparison group, cost hurdle, disclosures. */
export type FindingProvenance = {
  level: EvidenceLevel;
  n: number;
  sample_flag: SampleFlag;
  period: DateRange;
  regime: string;
  comparison_group: string;
  cost_hurdle_pct: number;
  cost_convention: string;
  data_source: string;
  universe: string;
  as_of: string;
  computed_by: string;
  computed_at: string;
  disclosures: string[];
};

/** FROZEN at publication. Never decided after the event. */
export type GradingRule = {
  kind: GradingKind;
  horizon_sessions: number;
  hurdle_pct: number;
  metric: string;
  right: string;
  wrong: string;
  inconclusive: string;
  frozen_at: string;
  rule_version: string;
  spec: Record<string, string | number | string[]>;
};

export type GradingState = {
  status: GradingStatus;
  due_session?: string | null;
  verdict?: Verdict | null;
  graded_at?: string | null;
  data_as_of?: string | null;
  realized_facts: Fact[];
  backfilled: boolean;
  /** the label the grade is published under — rendered verbatim */
  record: string;
  continues?: string | null;
  void_reason?: string | null;
  /**
   * where `due_session` came from: `session_calendar: …` (the engine, from the sealed data)
   * or `projected: …` (the API, over the exchange calendar, because the seal had not reached
   * the horizon). A projection is a calendar estimate, never the date the grade is judged on.
   */
  due_session_basis?: string | null;
};

/** Digit-free for BOTH authors; every number is a `{{fact:…}}` reference. */
export type Narrative = {
  headline: string;
  body: string;
  produced_by: Author;
  model?: string | null;
  prompt_version?: string | null;
  at: string;
  fact_refs: string[];
};

export type UsefulnessScore = {
  total: number;
  evidence_strength: number;
  novelty: number;
  trader_relevance: number;
  magnitude: number;
  threshold: number;
  version: string;
};

export type Finding = {
  id: string;
  edition_date: string;
  rank: number;
  tier: Tier;
  template_id: string;
  question: string;
  subject: string;
  subject_kind: SubjectKind;
  decision: Decision;
  decision_reason: string;
  narrative: Narrative;
  facts: Fact[];
  key_fact_refs: string[];
  provenance: FindingProvenance;
  grading_rule: GradingRule;
  grading: GradingState;
  usefulness: UsefulnessScore;
  related_symbols: string[];
  follow_up_questions: string[];
  disclosure: string;
  backfilled: boolean;
  continues?: string | null;
};

export type ScoreCounts = {
  right: number;
  wrong: number;
  inconclusive: number;
  n: number;
  sample_flag: SampleFlag;
};

export type Scoreboard = ScoreCounts & {
  pending: number;
  by_template: Record<string, ScoreCounts>;
  as_of: string;
  forward: ScoreCounts;
  backfilled: ScoreCounts;
  n_total: number;
  n_independent: number;
  continued: number;
  regraded: number;
  void: number;
  record_label: string;
};

export type FeedResponse = {
  /** the code that computed this edition: `pathfinder_research@…+code.<hash>`, plus `; pathfinder_experiments@…` when cards ride on it */
  engine_version?: string | null;
  /** `pathfinder_feed@<semver>+research_store.<n>+experiments_store.<n>` */
  schema_version?: string | null;
  edition_date: string;
  data_as_of: string;
  generated_at: string;
  regime: string;
  universe_scanned: number;
  candidates_considered: number;
  published_count: number;
  continued_count: number;
  usefulness_threshold: number;
  what_matters_now: Finding[];
  discoveries: Finding[];
  scoreboard: Scoreboard;
  llm_provider: string;
  disclosure: string;
  backfilled: boolean;
  record_label: string;
  experiment_cards: ExperimentCard[];
  experiments_scoreboard?: ExperimentScoreboard | null;
};

// ── S2: the experiment loop ──────────────────────────────────────────────────

export type ExperimentState = 'testing' | 'buried' | 'proposed';
export type ComparisonCategory = 'stronger' | 'weaker' | 'inconclusive' | 'failed' | 'void' | 'pending';

/** The seven-line customer narrative, in order. */
export type ExperimentBeat =
  | 'noticed'
  | 'researched'
  | 'history_showed'
  | 'decided'
  | 'happened'
  | 'learned'
  | 'next';

export type ExperimentGradingRule = {
  kind: 'experiment_edge';
  horizon_sessions: number;
  hurdle_pct: number;
  min_trades: number;
  metric: string;
  right: string;
  wrong: string;
  inconclusive: string;
  void: string;
  frozen_at: string;
  rule_version: string;
  spec: Record<string, string | number | string[]>;
};

/** Recorded UP FRONT and never revised. */
export type Expectation = {
  frozen_at: string;
  seal: string;
  metric: string;
  expectancy_net_pct: number;
  expectancy_2x_slippage_net_pct: number;
  hit_rate_pct: number;
  median_net_pct: number;
  n: number;
  signal_days: number;
  period: DateRange;
  trailing_expectancy_net_pct?: number | null;
  trailing_n: number;
  trailing_period?: DateRange | null;
  discovery_expectancy_2x_slippage_net_pct?: number | null;
  discovery_n: number;
  edge_vs_baseline_pct?: number | null;
  baseline_n: number;
  placebo_p?: number | null;
  placebo_draws: number;
  cluster_t?: number | null;
  hurdle_pct: number;
  computed_by: string;
  sample_flag: SampleFlag;
  // ── S2 re-audit, additive ──
  /** N1: the population the FROZEN expectation is measured on — the book's own selection (the strategy the book trades) */
  population?: string;
  /** every resolved firing of the rule on the window */
  signals_fired?: number | null;
  /** firings the book's limits could not take */
  signals_skipped?: number | null;
  /** CONTEXT, not the expectation: the same rule over every firing equal-weighted (the S1 card's population) */
  equal_weighted_expectancy_net_pct?: number | null;
  equal_weighted_n?: number;
  /** N3: share of the window's net P&L carried by its three best signal days (defined when the total is positive) */
  top3_days_share_pct?: number | null;
  /** N3: expectancy with the best signal day removed */
  expectancy_without_best_day_net_pct?: number | null;
  trailing_top3_days_share_pct?: number | null;
  trailing_expectancy_without_best_day_net_pct?: number | null;
  /** N8: the statistic the placebo compares (winsorised on both sides) */
  placebo_convention?: string | null;
  /** N8: binomial standard error of `placebo_p` at `placebo_draws` */
  placebo_se?: number | null;
  /** N5: CR3 (small-cluster corrected), judged against Student's t(G-1) */
  cluster_t_kind?: string | null;
};

export type ForwardResult = {
  as_of: string;
  label: string;
  capital_inr: number;
  n_closed: number;
  n_open: number;
  signal_days: number;
  signals_seen: number;
  signals_taken: number;
  mean_net_pct?: number | null;
  hit_rate_pct?: number | null;
  book_return_pct?: number | null;
  max_drawdown_pct: number;
  current_drawdown_pct: number;
  n_unresolved: number;
  sample_flag: SampleFlag;
  /** N9: the ONE convention every S2 drawdown is stated in */
  drawdown_convention?: string;
};

export type ExpectedVsActual = {
  expected_net_pct: number;
  actual_net_pct?: number | null;
  gap_pct?: number | null;
  category: ComparisonCategory;
  statement: string;
  fact_refs: string[];
};

export type LearningView = {
  statement: string;
  fact_refs: string[];
  level: LearningLevel;
  trials_evaluated: number;
  trials_passing: number;
  adopted_rule?: string | null;
  buried: boolean;
  next_action: 'continue' | 'revise' | 'bury' | 'propose';
};

export type PeriodView = {
  period_no: number;
  start?: string | null;
  sessions: number;
  end?: string | null;
  due?: string | null;
  status: 'pending' | 'open' | 'graded' | 'void';
  forward: ForwardResult;
  grading_rule: ExperimentGradingRule;
  verdict?: Verdict | null;
  grader_version?: string | null;
  graded_at?: string | null;
  data_as_of?: string | null;
  realized_facts: Fact[];
  expected_vs_actual?: ExpectedVsActual | null;
  learning?: LearningView | null;
  backfilled: boolean;
  record: string;
};

export type VersionView = {
  version: number;
  created_edition: string;
  rule_text: string;
  conditions: string[];
  horizon_sessions: number;
  change: string;
  why: string;
  level: LearningLevel;
  validation?: string | null;
  trials_for_this_version: number;
  expectation: Expectation;
  periods: PeriodView[];
  status: 'open' | 'superseded' | 'buried';
  backfilled: boolean;
};

export type ExperimentStoryLine = Narrative & { beat: ExperimentBeat };

export type ExperimentScoreCounts = ScoreCounts & { void: number };

/** The PUBLIC card: theme + evidence + seven beats. No constituent field exists. */
export type ExperimentCard = {
  id: string;
  opened_edition: string;
  news_edition: string;
  state: ExperimentState;
  family: string;
  theme: string;
  source_finding_id: string;
  evidence: FindingProvenance;
  story: ExperimentStoryLine[];
  facts: Fact[];
  versions_count: number;
  trials_total: number;
  periods_graded: number;
  score: ExperimentScoreCounts;
  latest_comparison: ComparisonCategory;
  backfilled: boolean;
  opened_backfilled: boolean;
  record_label: string;
  llm_provider: string;
  disclosure: string;
  /** N4: every trial this experiment's FAMILY has ever had as of this card's edition — the count the family-wise bar divides by; never restarts */
  family_trials_all_time?: number;
};

export type GateView = {
  name: string;
  passed: boolean;
  value?: number | null;
  bar?: number | null;
  statement: string;
  fatal: boolean;
  /** N2/N5: the statistic could not be computed on the record it has (too few signal days) — not passed, not a measured failure */
  insufficient?: boolean;
};

export type TrialView = {
  trial_no: number;
  context: string;
  signature: string;
  rule_text: string;
  passed: boolean;
  adopted: boolean;
  reason: string;
  expectancy_net_pct?: number | null;
  trailing_expectancy_net_pct?: number | null;
  n: number;
  gates: GateView[];
};

export type BasketView = {
  description: string;
  constituents_visibility: 'withheld_pending_ra_review' | 'in_app_ra_reviewed';
  ra_review_state: string;
  constituents: string[];
};

export type ProposalView = {
  proposed_edition: string;
  version: number;
  target_agent: 'trader' | 'investor';
  status: 'proposed_awaiting_human' | 'blocked_unsigned_constitution';
  gates: GateView[];
  incumbent: string;
  decided_by: 'engine';
  human_gate: string;
};

export type ExperimentRecord = ExperimentCard & {
  question: string;
  current_rule_text: string;
  direction: Direction;
  versions: VersionView[];
  trials: TrialView[];
  basket: BasketView;
  worth_testing_gates: GateView[];
  change_log: ChangeLogEntry[];
  post_mortem?: PostMortem | null;
  proposal?: ProposalView | null;
  constitution_version: string;
};

/** An S1 finding the gate did NOT open — on the record with its trial count and reason. */
export type RejectedCandidate = {
  finding_id: string;
  edition_date: string;
  template_id: string;
  family?: string | null;
  trials_evaluated: number;
  reason: string;
  best_rule_text?: string | null;
  /** N1: on the BOOK-selected population (the strategy the book trades), not every firing */
  best_expectancy_net_pct?: number | null;
  best_failed_gates: string[];
  /** N4: the family's trial count after this evaluation, across every finding and retry */
  family_trials_all_time?: number | null;
};

export type ExperimentScoreboard = {
  right: number;
  wrong: number;
  inconclusive: number;
  n: number;
  void: number;
  pending: number;
  forward: ScoreCounts;
  backfilled: ScoreCounts;
  by_family: Record<string, ScoreCounts>;
  experiments_testing: number;
  experiments_buried: number;
  experiments_proposed: number;
  candidates_not_opened: number;
  trials_total: number;
  as_of: string;
  record_label: string;
  sample_flag: SampleFlag;
};

export type ExperimentsResponse = {
  as_of: string;
  count: number;
  items: ExperimentCard[];
  not_opened: RejectedCandidate[];
  scoreboard: ExperimentScoreboard;
  llm_provider: string;
  engine_version: string;
  disclosure: string;
};

export type ApiErrorBody = {
  /** `use`: for `not_served_by_source`, the paths the configured source DOES serve */
  error: { code: string; message: string; request_id?: string | null; use?: string[] | null };
};
