// API contract for the strategy registry, Discover Strategies and Falcon (docs/FALCON_DISCOVER_SPEC.md §5, §8).
// Shared by the backend (server/kanida_pilot/strategies.py, falcon.py) and the frontend (src/discover, src/falcon, src/admin). Change only with all three in step.

export type Audience='trader'|'investor'|'both';
export type BlockKind='chart'|'quant'|'results'|'options'|'candlestick'|'events'|'price_action'|'harmonic';
export type Side='long'|'short';
export type CellStatus='tested'|'small_test_sample'|'no_validated_rule'|'no_occurrences'|'unknown';

export type StrategyDef={
 key:string;              // e.g. "falling_wedge-1D-long"
 block_key:string;        // e.g. "chart"
 name:string;             // e.g. "Falling Wedge breakout · 1D"
 description:string;
 tags:string[];           // e.g. ["Chart patterns","Bullish","1D"]; picker chips are built from these
 /** The registry has accepted both sources since the 107-pattern catalogue landed. A served research row is a
  * `ResearchStrategyDef` (it carries pattern_id/variant/family/research_run as well). */
 source_type:SourceType;
 pattern:string;          // stored: the scanner pattern id ("falling_wedge"); research: the catalogue pattern_id ("CH05")
 pattern_name:string;
 timeframe:string;        // "1H" | "4H" | "1D" | "1W"
 side:Side;
 audience:Audience;       // 1H/4H/1D → trader, 1W → investor by default
 min_trades:number;       // sample needed before a 95% low is shown as evidence (default 10)
 default_slot:'A'|'B'|null;  // in the Discover catalog of a researched block: the LIVE default (most active setups), see below
 order:number;
 enabled:boolean;
 /** Discover catalog only, researched blocks: the owner's registry slot. `default_slot` there is replaced by the
  * two strategies with the most active (forming + confirmed) setups right now, falling back to this registry
  * slot only when fewer than two have one. A user's saved choice still wins (discover/logic.resolveSlot). */
 registry_default_slot?:'A'|'B'|null;
};

export type StrategySummary=StrategyDef&{
 found:number;                  // NIFTY 500 (or requested universe) stocks where it is found now
 best_low_pct:number|null;      // best 95% low among found rows with n >= min_trades
 positive_low_count:number;     // rows with 95% low > 0 and n >= min_trades
 tested_count:number;           // rows whose backtest cell status is "tested"
};

export type BlockDef={key:string;title:string;description:string;kind:BlockKind;order:number;enabled:boolean;
 /** Why this block has nothing to list, in the server's words. Not an error. */
 unavailable?:string|null;
 /** A stored block while the scanner runs the researched set is now OMITTED from the Discover catalog (its 10
  * patterns are a subset of the 107), so a served block always carries `false`. If an older server sends `true`,
  * Discover renders nothing for it. The admin registry still lists the block. */
 superseded?:boolean;superseded_note?:string|null};

export type DataFreshness={data_end:string|null;age_days:number|null;stale:boolean;scanned_at:string|null;
 /** Which detector set the scanner is running: 'legacy' (the stored 10) or 'research' (the 107). */
 pattern_set?:'legacy'|'research';
 // How the scanner answered (BACKLOG item 2a, server/kanida_pilot/strategies.py `freshness`). The scanner takes
 // minutes to load its scan and can restart mid-day; while it is quiet the pilot replays the LAST successful
 // read and stamps it here, so the page can show that scan and say when it was taken. These fields describe the
 // CONNECTION only - `data_end`/`age_days` above still describe the scan itself.
 /** True when this body is the last good copy rather than a fresh read. */
 served_from_cache?:boolean|null;
 /** When that copy was taken (naive IST, "2026-09-17 15:30:12"). */
 cached_at?:string|null;
 /** The scan's own as-of time inside that copy. */
 cached_as_of?:string|null;
 /** One plain sentence: why the scanner did not answer. Never a stack trace. */
 upstream_error?:string|null;
 /** How long the scanner has been quiet; past RECONNECT_GRACE_SECONDS the app stops calling it "reconnecting". */
 upstream_down_seconds?:number|null;
 reconnecting?:boolean|null;
 /** The scanner is unreachable AND there is nothing cached to show. */
 unreachable?:boolean|null};

export type Catalog=DataFreshness&{
 registry_version:number;
 universe:{key:string;label:string;count:number};
 blocks:(BlockDef&{strategies:StrategySummary[]})[];
 /** Whether the scanner is running the researched pattern set, so research cards can show live detections. */
 live?:LiveState;
};

export type StrategyRow={
 symbol:string;company:string;sector:string|null;
 match_id:string;timeframe:string;side:Side;direction:string;state:string;
 price:number|null;candle_end:string|null;
 low_pct:number|null;           // history[side].reference.expectancy_ci95[0] (hold-period history, after costs)
 high_pct:number|null;          // expectancy_ci95[1]
 avg_pct:number|null;           // reference.expectancy_pct
 win_rate:number|null;          // reference.win_rate (context only)
 n:number;                      // reference.n
 sample_label:string;           // "Small sample" | "Moderate sample" | "Larger sample" | "No history"
 status:CellStatus;             // history[side].status
 test:{n:number;expectancy_pct:number|null;expectancy_ci95:[number,number]|null}|null;
 evidence_basis:'tested_rule'|'hold_period_history'|'none';
};

export type StrategyResults=DataFreshness&{
 strategy:StrategySummary;
 universe:{key:string;label:string;count:number};
 source:'stored_scan';
 rows:StrategyRow[];            // sorted: low_pct desc (nulls last), then n desc, then symbol
 total:number;
 /** Why the set is empty, when it is empty for a REASON (e.g. the scanner runs the researched set). An empty
  * set with a reason is a different claim from an empty set without one, and the card says which. */
 unavailable?:string|null;
};

export type InsightKind='setup'|'caution'|'cluster'|'context';
export type Insight={
 rank:number;kind:InsightKind;audience:'trader'|'investor';
 headline:string;why:string;chips:string[];
 symbol?:string;strategy_key?:string;block_key?:string;match_id?:string;
 low_pct?:number|null;avg_pct?:number|null;n?:number;sample_label?:string;
 deeplink:string;               // "/discover?b=chart&a=<strategy_key>&sel=A&s=<SYMBOL>"
};

export type ScanStageKey='boot'|'sentiment'|'noise'|'strategies'|'walkforward'|'probability'|'compose';
export type ScanStage={
 key:ScanStageKey;title:string;status:'waiting'|'running'|'done'|'unavailable'|'failed';
 started_at:number|null;finished_at:number|null;
 lines:string[];                // human reference lines built from real numbers
 metrics:Record<string,number|string|null>;
 references?:string[];          // e.g. strategy names / symbols to flicker in the animation
};

export type MarketContext={basis_date:string|null;stocks:number;above_50dma_pct:number|null;advancers:number|null;decliners:number|null;unchanged:number|null;nifty_trend:'up'|'down'|'flat'|null;note:string};

export type ScanJob={
 id:string;status:'running'|'done'|'failed'|'cancelled';
 started_at:number;finished_at:number|null;error?:string;
 stages:ScanStage[];
 result:(DataFreshness&{context:MarketContext;traders:Insight[];investors:Insight[];counts:Record<string,number>})|null;
};

// --- Researched pattern catalogue (docs/pattern_research/IMPLEMENTED_CATALOGUE.json) -------------------------
// 107 catalogue entries / 262 direction-variant combinations x 4 timeframes = 1,048 strategies, served from a
// precomputed index (server/kanida_pilot/research_index.py) by the SAME catalog/results endpoints.

export type SourceType='stored_pattern'|'research_pattern';
export type PatternFamily='chart'|'candlestick'|'price_action'|'harmonic';
/** EVIDENCE_SERVING_CONTRACT.md §6. `loading` is not a contract row: the outcome run has not reached this cell. */
export type EvidenceState='insufficient_history'|'no_occurrences'|'no_walkforward_trades'|'limited_sample'
 |'walkforward_result'|'incompatible'|'requires_review'|'loading';
/** The contract's label table, verbatim. Mirrored in server/kanida_pilot/cards.py; change only in step. */
export const EVIDENCE_LABELS:Record<EvidenceState,string>={
 insufficient_history:'Not enough historical data',
 no_occurrences:'No occurrences in this historical sample',
 no_walkforward_trades:'No selected walk-forward trades',
 limited_sample:'Limited historical sample',
 walkforward_result:'Historical walk-forward result',
 incompatible:'Incompatible historical evidence',
 requires_review:'Historical data requires review',
 loading:'Evidence loading',
};
export const WALKFORWARD_MIN=20;
/** Forward-return display grid per timeframe (the outcome engine's `display_grid.declared`). */
export const DISPLAY_GRID:Record<string,number[]>={'1H':[1,2,4,8,12,24],'4H':[1,2,3,5,8,10],'1D':[1,2,3,5,10],'1W':[1,2,4,8,12]};

// --- what the owner's add-strategy form may offer (GET /api/admin/strategy-sources) ------------------------
// The registry's OWN accept-list, not the detector set the scanner happens to be running. The form used to build
// its chips from /api/state and post them all as `stored_pattern`; on the researched set every one of those was a
// catalogue id the stored path refuses, so every add answered 400 (BACKLOG item 4).
/** One researched variant of a catalogue pattern, and the sides it was actually studied on. */
export type SourceVariant={id:string;label:string;state:'setup'|'confirmed';sides:Side[]};
export type SourcePattern={
 id:string;               // stored: "falling_wedge" · research: the catalogue pattern_id, e.g. "CH05"
 name:string;description?:string;family?:PatternFamily;
 block_key:string|null;   // the block this pattern belongs in; null when the server has no block for its family
 sides?:Side[];           // stored patterns only (research sides live on the variant)
 variants:SourceVariant[];// empty for a stored pattern
};
/** `available:false` carries the server's own `reason`; the form then offers the other source only. */
export type StrategySource={available:boolean;reason:string|null;run?:string|null;block_key?:string;
 timeframes:string[];patterns:SourcePattern[]};
export type StrategySources={stored:StrategySource;research:StrategySource};

export type ResearchStrategyDef=Omit<StrategyDef,'source_type'>&{
 source_type:'research_pattern';
 pattern_id:string;variant:string;state:'setup'|'confirmed';family:PatternFamily;research_run:string;
};
export type ResearchSummary=ResearchStrategyDef&{
 // THREE different things, never merged into one number:
 //  1. DETECTED TODAY - `live_detection` + `detections_*`, counted from the scanner's detection ledger
 //     (docs/LIVE_DETECTION.md §5A). `live_detection:false` means the scanner is not running the research
 //     pattern set, and then every `detections_*` is null - never 0, which would read as "we looked and
 //     found nothing".
 //  2. RESEARCHED HISTORY - `history_*`, `last_seen`, `researched_stocks`, `occurrences`. `researched_stocks`
 //     is the size of the history list and is NEVER a detection count.
 //  3. EVIDENCE - `evidence_*`, `cells_*`, `tier_*`, `best_edge_*`, from the outcome engine.
 live_detection:boolean;
 detections_today:number|null;    // signal bar closed today (IST), any lifecycle state
 detections_week:number|null;     // ... within the last 7 days
 detections_live:number|null;     // standing in forming/confirmed right now - the live book
 detections_symbols:number|null;  // distinct stocks behind the largest single lifecycle state (a floor)
 detections_as_of:string|null;    // newest candle the scanner detected this strategy on
 detections_scanned_at:string|null;// when the scan that last saw it committed
 detections_last_detected?:string|null;// newest detection the ledger holds, any state ("last seen")
 history_today:number|null;    // RESEARCH history: last occurred on the newest bar the index holds
 history_week:number|null;     // ... within 7 calendar days of it
 researched_stocks:number|null;// stocks in the history list for this combination
 evidence_ready:number;        // of those, how many already have an outcome row
 evidence_total:number|null;
 // Rows by how USABLE their evidence is. Most indexed cells are measured but carry no accepted walk-forward
 // selection, so presence of an outcome row is not the same as having something to show.
 tier_result:number|null;      // an accepted out-of-sample result (evidence_tier 0)
 tier_limited:number|null;     // an out-of-sample result on a thin sample (evidence_tier 1)
 tier_history:number|null;     // measured, but nothing selected: history only (evidence_tier 2+)
 evidence_summary:string|null; // composed server-side, e.g. "26 stocks with an out-of-sample result · 469 with history only"

 cells:number|null;            // researched stock cells for this combination
 cells_with_evidence:number;   // cells the outcome engine has produced a row for
 cells_tested:number;          // cells with an accepted walk-forward selection
 cells_loading:number|null;    // cells the outcome run has not reached yet
 occurrences:number|null;
 best_edge_low_pct:number|null;// best 95% low of the edge over the stock alone, among cells with n >= 20
 best_edge_symbol:string|null;
 beats_baseline:number;
 last_seen:string|null;
 evidence_pending:boolean;     // the index has no row yet - never reported as "0 found"
};
export type ResearchRow={
 symbol:string;company:string;sector:string|null;market_cap_tier:string|null;
 has_evidence:boolean;               // false = the outcome run has not reached this cell; the row shows a chip, not dashes
 // Ranking tier: 0 accepted out-of-sample result · 1 limited sample · 2 measured but nothing selected ·
 // 3 nothing to report · 4 not computed yet. Rows are served already sorted by this, then by edge low.
 evidence_tier:number;
 // True when THIS ROW's three columns (edge low / out-of-sample average / walk-forward n) actually hold values.
 // When false, render `label` - the contract's reason - instead of three dashes.
 has_numbers:boolean;
 occurrences:number|null;walkforward_n:number|null;
 walkforward_mean_pct:number|null;   // out-of-sample mean net return after costs
 win_rate:number|null;               // out-of-sample win rate
 edge_mean_pct:number|null;          // mean net return MINUS entering the same stock on any eligible bar
 edge_low_pct:number|null;edge_high_pct:number|null;   // its 95% interval (the ranking key)
 beats_baseline:boolean;baseline_mean_pct:number|null;
 p_target_first:number|null;p_stop_first:number|null;
 selected_horizon:number|null;q_value:number|null;p_value:number|null;
 last_seen:string|null;found:boolean;
 evidence_state:EvidenceState;label:string;research_status:string|null;selection_status:string|null;
};
export type ResearchIndexState={
 built_at:string|null;age_seconds:number|null;building:boolean;stale:boolean;error:string|null;
 research_run:string|null;outcome_run:string|null;indexed_outcome_status:string|null;
 current_outcome_run:string|null;current_outcome_status:string|null;
 source_run:string|null;snapshot_id:string|null;engine_version:string|null;
 strategies:number;rows:number;symbols_research:number;symbols_with_evidence:number;
 evidence_coverage_pct:number;build_seconds:number;latest_seen:string|null;
 latest_by_timeframe:Record<string,string>;  // the research run's data end per timeframe, e.g. {'1D':'2026-09-15'}
 publication:{run:string;publication_status:string;source_quality_status:string;decided_by:string|null;decided_at:string|null;released:boolean};
};
export type ResearchResults={
 strategy:ResearchSummary;source:'research_index';rows:ResearchRow[];total:number;offset:number;
 live_detection:boolean;live:LiveState;
 coverage:{ready:number;total:number;tier_result:number;tier_limited:number;tier_history:number};
 universe:{key:string;label:string;count:number};
 // A research card stands on TWO clocks. `data_end`/`evidence_end` is the research run's own data end for THIS
 // strategy's timeframe (a real date, never null when the index is built), and the live detection time comes
 // from `live.as_of`. `stale` is always false here: a frozen research run is not stale prices, and index
 // staleness lives on `research.stale`, which means something else entirely.
 data_end:string|null;age_days:number|null;stale:boolean;scanned_at:string|null;
 evidence_end:string|null;evidence_age_days:number|null;
 // Server-composed footer naming both clocks, e.g.
 // "Detections live · 16 Sep 15:30 · evidence from research to 15 Sep 2026".
 // It never says "unknown age": when a date is missing it names which one.
 provenance:string;
 research:ResearchIndexState;
};

// --- live detections (docs/LIVE_DETECTION.md) -------------------------------------------------------------
// The scanner's own detection ledger, served read-only by server/kanida_pilot/detections.py. `available:false`
// means the scanner is not running the research pattern set (or its cache is elsewhere): Discover then serves
// the researched history and says so. It NEVER means "nothing was detected".
export type DetectionState='forming'|'confirmed'|'invalidated'|'expired';
export type DetectionScope='today'|'live'|'week';
/** LIVE_DETECTION.md §6. `identity_match` only means the identity PERMITS a research card. */
// LIVE_DETECTION.md §6 defines the first three. `not_studied` is the app's own, more precise fourth: the
// identity matches but the research run never covered this (symbol, timeframe, pattern, variant, side) at
// all, because the scanner's universe is wider than the run's. It is still "no compatible evidence".
export type DetectionEvidenceStatus='identity_match'|'detector_mismatch'|'unavailable'|'not_studied';
export type DetectionEvidence={status:DetectionEvidenceStatus;note:string;research_run:string|null;
 research_detector_spec_hash:string|null;detector_spec_hash:string|null;label?:string|null};
export type LiveState={
 available:boolean;day?:string;as_of?:string|null;scanned_at?:string|null;
 detections?:number;live?:number;symbols?:number;states?:Record<DetectionState,number>;
 timeframes?:Record<string,{timeframe:string;detections:number;live:number;last_seen:string|null;as_of:string|null}>;
 detector_spec_hash?:string|null;mixed_detector_identity?:boolean;ledger?:string|null;
 error?:string|null;reason?:string;
};
export type LiveDetection={
 detection_id:string;episode_id:string;
 symbol:string;company:string;sector:string|null;
 timeframe:string;pattern_id:string;variant:string;side:Side;family:PatternFamily;pattern_name:string;
 state:DetectionState;state_label:string;state_reason:string|null;state_at:string|null;
 detector_state:'setup'|'confirmed'|null;live:boolean;bars_since_state:number|null;
 /** Completed candles since the signal bar - the row's age. */ bars_since_signal?:number|null;
 direction:string|null;fit_score:number|null;score:number|null;price:number|null;atr:number|null;
 formation_start:string;detected_at:string;signal_at:string;confirmed_at:string|null;
 as_of:string;first_seen:string;last_seen:string;detected_day:string|null;current:boolean;
 detector_spec_hash:string;live_rules_version:string;
 /** Non-empty exactly when the detector published nothing drawable; then the card says so, not a blank chart. */
 geometry_note:string;drawable:boolean;quality_tags:string[];
 /** The scanner's cell id. NOT unique across episodes - `detection_id` is. */
 match_id:string;
 evidence:DetectionEvidence;evidence_compatible:boolean;
};
export type LiveDetectionResults={
 strategy:ResearchSummary;source:'detection_ledger';scope:DetectionScope;
 live:LiveState;live_detection:boolean;
 rows:LiveDetection[];total:number;offset:number;limit:number;
 evidence:DetectionEvidence;
};

// --- the trader evidence card ----------------------------------------------------------------------------
export type CardIdentity={strategy_key:string;name:string;symbol:string;timeframe:string;pattern_id:string;
 variant:string;side:Side;state:'setup'|'confirmed';family:PatternFamily;definition_version:string|null;
 research_run:string;outcome_run:string|null;snapshot_id:string|null;source_run:string|null;
 /** Set when the card was opened from a live detection; its identity had to match for numbers to be served. */
 detection_id?:string|null};
export type CardSummary={
 occurrences:number|null;sample_label:string|null;horizon:number|null;horizon_bars_word:string;
 win_rate_pct:number|null;baseline_win_rate_pct:number|null;
 win_rate_text:string|null;                 // "61% vs 58% for the stock alone"
 win_rate_scope:'out_of_sample'|'all_history_descriptive';
 median_net_return_pct:number|null;mean_net_return_pct:number|null;baseline_mean_net_return_pct:number|null;
 median_mfe_pct:number|null;median_mae_pct:number|null;walkforward_n:number|null;
 flatten_low:number|null;flatten_high:number|null;
 window_text:string|null;                   // "most of the move happens within 5-8 4H candles"
};
export type CardBarriers={
 barrier_id:string|null;target_pct:number|null;stop_pct:number|null;n:number|null;
 p_target_first:number|null;p_stop_first:number|null;p_neither:number|null;
 headline:string|null;                      // "hit +2% before -1%: 64% - hit -1% first: 29% - neither: 7%"
 tie_rule:string|null;tie_rule_text:string;
 median_bars_to_target:number|null;median_bars_to_stop:number|null;
 both_touched_same_bar_n:number|null;undetermined_n:number|null;sample_label:string|null;
};
export type CurvePoint={h:number;n:number|null;median_net_return_pct:number|null;mean_net_return_pct:number|null;
 win_rate_pct:number|null;baseline_mean_net_return_pct:number|null;median_mfe_pct:number|null;median_mae_pct:number|null};
export type CardCurve={grid:number[];declared:number[];dropped_beyond_horizon:number[];max_horizon:number|null;
 points:CurvePoint[];peak_h:number|null;selected_h:number|null;bars_word:string;basis:string};
export type CardBucket={bucket:string;name:string;n:number;enough:boolean;note:string|null;
 win_rate_pct:number|null;mean_net_return_pct:number|null;median_net_return_pct:number|null;
 baseline_mean_net_return_pct:number|null;baseline_n:number|null;baseline_scope:string|null;
 diff_mean_net_return_pct:number|null;q_value:number|null};
export type CardConditions={minimum_sample:number;horizon:number|null;scope:string;
 groups:{dimension:string;title:string;definition:string|null;buckets:CardBucket[]}[]};
export type CardOccurrence={date:string|null;entry_time:string|null;entry_price:number|null;
 next_move_pct:number|null;best_move_pct:number|null;worst_move_pct:number|null;
 bars_held:number|null;outcome:string|null;horizon:number|null};
export type CardLast={rows:CardOccurrence[];read:string|null;barrier_id:string|null;tie_rule:string|null;note:string|null};
export type CardSignificance={q_value:number|null;p_value:number|null;fdr_trials:number|null;discovery_q10:number|null;line:string};
/** Section order is fixed by the owner's spec and rendered in exactly this order. */
export type CardSection='summary'|'barriers'|'forward_curve'|'conditions'|'last_occurrences'|'honesty';
export type EvidenceCard={
 identity:CardIdentity;evidence_state:EvidenceState;label:string;
 research_run:string;outcome_run:string|null;snapshot_id:string|null;engine_version:string|null;
 publication:ResearchIndexState['publication'];review_required:boolean;review_label:string|null;
 occurrences:number|null;sections:CardSection[];
 summary:CardSummary|null;barriers:CardBarriers|null;forward_curve:CardCurve|null;
 conditions:CardConditions|null;last_occurrences:CardLast|null;
 honesty:string[];significance:CardSignificance|null;note:string|null;
 strategy:ResearchStrategyDef;research:ResearchIndexState|null;
 /** Present only when the card was opened from a live detection. `live_evidence.status` decides whether
  * this cell's numbers were served at all; anything but `identity_match` leaves the card's numbers absent
  * and its label at "Incompatible historical evidence". */
 detection?:LiveDetection|null;live_evidence?:DetectionEvidence|null;
};
