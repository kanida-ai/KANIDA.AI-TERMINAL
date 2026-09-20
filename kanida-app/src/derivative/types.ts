// Shapes the Derivative tab reads from the pilot (docs/DERIVATIVES_SPEC.md §3-§4, served by
// server/kanida_pilot/derivatives.py). Every number here was CAPTURED; nothing on this tab is a forecast (§5).
// A signal the metrics worker has not written yet arrives as null and is named in `missing` - it is rendered as
// "no baseline" or a dash, never as zero.
export type Buildup='long_buildup'|'short_buildup'|'short_covering'|'long_unwinding';
export type Floors={premium_cr:number;oi_lots:number;last_price:number};
/** Which source answered: D2's reader module, or the `metrics` rows it writes. */
export type DerivativeSource='metrics_module'|'store'|'none';
export type Envelope={available:boolean;captured:boolean;as_of:string|null;floors:Floors;floors_text:string;
 source:DerivativeSource;missing:string[];empty_text:string;empty_reason:string|null;baseline_sessions_required:number;
 /** THE ONE ANALYSIS CONTEXT this card was resolved under, when the route carries one. */
 context?:AnalysisContext|null;
 /** What was MEASURED at this reading, on the routes that report it. */
 capture?:CaptureHealth|null};

// --- THE ONE ANALYSIS CONTEXT -----------------------------------------------------------------------------
// Every card used to resolve its own instrument, expiry, session and "latest", and the page then put the
// answers side by side under one heading. On 18 Sep 2026 the max-pain headline printed a strike from the
// 15:45 reading, a spot from the 11:30 reading and a distance that was true only of the 11:30 PAIR. Each
// number was captured and correct; the sentence they made was false.
//
// So a card now says WHICH boundary it was resolved at, and every figure under it carries the reading it was
// actually observed at. Two figures observed an hour apart are never combined into a third.
/** 'live' = the session on screen is today in IST and will grow at the next capture. */
export type AnalysisMode='live'|'historical';
export type AnalysisContext={underlying:string|null;expiry:string|null;session:string|null;
 /** The 15-min reading this card was resolved AT OR BEFORE. */
 at:string|null;
 /** The newest reading this underlying has, so a card behind the live edge can say so. */
 newest_at:string|null;is_newest:boolean;
 timezone:string;mode:AnalysisMode;source:string;
 /** Counted from `session`, never from today. `days_to_expiry_basis` is the convention, printed not paraphrased. */
 days_to_expiry:number|null;days_to_expiry_basis:string;context_text:string};

// --- DERIVATIVE CAPTURE HEALTH ----------------------------------------------------------------------------
// FIVE STATES, KEPT APART. At the 15:45 reading of 18 Sep 2026 the store held 10,552 contract rows with zero
// spots and zero premiums - the capture had died at 11:30 and the rest of the session was rebuilt from
// 15-minute candles, which carry no traded-price average. The screen said "No contract cleared the liquidity
// floors", which a trader reads as "the market is quiet": the opposite of what happened. Nothing was measured.
export type CaptureState='missing_capture'|'partial_capture'|'complete'|'no_eligible_rows'|'filtered_out'|'failed';
export type CaptureField={present:number|null;rows:number;share:number|null;column:boolean;label:string};
export type CaptureHealth={state:CaptureState;state_text:string;at:string|null;rows:number;
 /** field -> how many rows at this reading carried it. Counted, never assumed. */
 coverage:Record<string,CaptureField>;
 /** Required fields with ZERO coverage: the ones that make this a partial capture. */
 missing_fields:string[];empty_fields?:string[];required_fields:string[];
 field_labels?:Record<string,string>|null;
 /** Where the reading came from. `candles_15m` is a reading the worker rebuilt, which carries no traded price. */
 source:string;
 /** NEVER true for a partial capture. A reading nothing was measured at cannot be healthy. */
 healthy:boolean;capture_text:string;states:CaptureState[];
 /** The newest reading attempted, the newest with any rows, and the newest whose required fields were all
  *  captured. The last is OFFERED to the reader; it is never silently substituted for the one on screen. */
 latest_attempted_at:string|null;latest_available_at:string|null;latest_complete_at:string|null;
 latest_complete_is_here?:boolean;
 /** How many contracts cleared the floors, and how many survived the reader's own filters. The pair is what
  *  separates "nothing qualified" from "your filters excluded everything". */
 cleared:number|null;matched:number|null};
/** One contract at one 15-minute reading. */
export type ContractRow={instrument_token:number|null;tradingsymbol:string;underlying:string;instrument_type:string;
 strike:number|null;expiry:string;lot_size:number|null;days_to_expiry:number|null;captured_at:string|null;
 last_price:number|null;oi:number|null;oi_lots:number|null;volume:number|null;average_price:number|null;
 premium_cr:number|null;price_change_15m_pct:number|null;oi_change_15m:number|null;oi_change_15m_pct:number|null;
 buildup_15m:Buildup|string|null;price_change_day_pct:number|null;oi_change_day:number|null;
 oi_change_day_pct:number|null;buildup_day:Buildup|string|null;
 /** §3.2: served only with at least `baseline_sessions_required` sessions behind it, else null + 'none'. */
 volume_ratio:number|null;volume_baseline_sessions:number|null;volume_baseline:'ok'|'none';
 volume_to_oi:number|null;previous_oi:number|null;basis:number|null;oi_vs_20d_avg:number|null;
 /** The underlying's price at this 15-min reading, when the metrics worker writes one; null = not captured,
  *  shown as a dash. */
 spot:number|null};
export type UnusualGroup={underlying:string;premium_cr:number;strike_count:number;calls:number;puts:number;
 expiries:string[];days_to_expiry:number|null;oi_change_day:number|null;strikes:ContractRow[]};
export type Unusual=Envelope&{rows:UnusualGroup[];total:number;floor_premium_cr:number;empty_note:string|null};
export type ChainRow={strike:number;ce:ContractRow|null;pe:ContractRow|null};
export type Chain=Envelope&{rows:ChainRow[];underlying:string;expiry:string;spot:number|null;
 /** Which reading the spot came from. A chain never borrows an earlier reading's spot to have one. */
 spot_at?:string|null;session?:string|null;days_to_expiry:number|null;days_to_expiry_basis?:string|null;
 total:number};
export type StrikeOi={strike:number;ce_oi:number|null;pe_oi:number|null;ce_oi_change_day:number|null;
 pe_oi_change_day:number|null;ce_buildup_day:string|null;pe_buildup_day:string|null};
export type OiByStrike=Envelope&{rows:StrikeOi[];underlying:string;expiry:string;spot:number|null;
 max_pain_strike:number|null;max_pain_distance:number|null;total_ce_oi:number|null;total_pe_oi:number|null;
 /** Which reading each of the pair came from, and why there is no distance when there is none. */
 spot_at?:string|null;max_pain_strike_at?:string|null;max_pain_distance_at?:string|null;
 max_pain_distance_reason?:string|null;max_pain_distance_withheld?:string|null;
 max_pain_distance_definition?:string|null;session?:string|null;
 days_to_expiry:number|null;days_to_expiry_basis?:string|null};
export type IndexPoint={captured_at:string|null;spot:number|null;pcr_oi:number|null;pcr_volume:number|null;
 total_ce_oi:number|null;total_pe_oi:number|null;max_pain_strike:number|null};
export type IndexRow={underlying:string;captured:boolean;captured_at:string|null;spot:number|null;
 pcr_oi:number|null;pcr_volume:number|null;max_pain_strike:number|null;max_pain_distance:number|null;
 /** STRIKE MINUS SPOT, the tab's one convention. This list used to serve the opposite sign. */
 max_pain_distance_reason?:string|null;max_pain_distance_withheld?:string|null;session?:string|null;
 total_ce_oi:number|null;total_pe_oi:number|null;expiry:string;days_to_expiry:number|null;series:IndexPoint[]};
export type Indices=Envelope&{rows:IndexRow[];max_pain_distance_definition?:string|null;
 distance_reason_text?:Record<string,string>|null;days_to_expiry_basis?:string|null};
export type Futures=Envelope&{rows:ContractRow[];total:number};
export type SeriesPoint={t:string|null;price:number|null;oi:number|null;volume?:number|null;
 ce_oi?:number|null;pe_oi?:number|null;pcr_oi?:number|null};
export type Series=Envelope&{points:SeriesPoint[];kind:'contract'|'underlying'|'none';underlying:string;
 tradingsymbol?:string;instrument_token?:number;expiry?:string;strike?:number|null;instrument_type?:string;
 price_label?:string;oi_label?:string};
/** One 15-minute reading of the grid. `delta_oi` is null when the reading, or the previous close, was not
 *  captured; `price` is null when that reading carried no last traded price. Neither is ever interpolated. */
export type GridPoint={at:string|null;oi:number|null;delta_oi:number|null;price:number|null};
/** The four things a slot's direction may say. "no baseline" is a state, not a fourth direction — it gets no chip. */
export type GridDirection='building'|'flat'|'unwinding'|'no baseline';
/** The same four states, read on the contract's OWN premium over the same window. */
export type GridPriceDirection='up'|'down'|'flat'|'no baseline';
/** Price and OI read together: what is happening, and what it means. Never a forecast (§5). */
export type GridFlow={price_direction:GridPriceDirection|string;oi_direction:GridDirection|string;
 what_label:string;meaning:string|null;detail:Record<string,unknown>};
/** One of the ten fixed slots. An unlisted strike is still a slot: `present:false` with its own `missing_text`. */
export type GridSlot={slot:string;option_type:'CE'|'PE';atm_offset:number;label:string;row:'calls'|'puts';
 present:boolean;tradingsymbol:string|null;instrument_token:number|null;strike:number|null;
 previous_close_oi:number|null;points:GridPoint[];direction:GridDirection|string;
 direction_detail:Record<string,unknown>;marks:number;marks_with_delta:number;latest_delta_oi:number|null;
 peak_abs_delta_oi:number|null;marks_with_price:number;latest_price:number|null;flow:GridFlow;
 missing_text:string|null};
/** §4, the owner's ΔOI block: ATM CE and the four strikes above it, ATM PE and the four below. */
export type OiGrid=Envelope&{rows:GridSlot[];underlying:string;expiry:string;session:string|null;
 spot:number|null;spot_symbol:string|null;atm_strike:number|null;
 atm_basis:{mark?:string|null;spot?:number|null;rule?:string}|null;marks:string[];total:number;
 days_to_expiry:number|null;points_source:'snapshots'|'candles_15m'|'read_api'|'none'|string;
 /** The owner's one sentence when there is nothing to draw, and the reader's own reason beside it. */
 empty_note:string|null;empty_detail:string|null;
 delta_oi_text:string;atm_text:string;direction_text:string;not_enough_marks:string;
 direction_lookback_marks:number;flat_fraction:number;grid_width:number;grid_slots:number;
 /** The second line, the reading under it, and the one line under the whole block. */
 price_text:string;flow_text:string;block_text:string;price_flat_fraction:number;
 block_balance_ratio:number;block_read:string};
/** One candle of a futures contract. A GAP arrives in this same series: `at` set, every price null and
 *  `gap: true`. It holds its place on the axis so the hole stays visible, and it is neither drawn nor counted
 *  as a stored bar — nothing is interpolated and no candle is carried forward into it. */
export type FuturesCandle={at:string|null;open:number|null;high:number|null;low:number|null;close:number|null;
 volume:number|null;oi:number|null;gap?:boolean};
/** One entry of the endpoint's `intervals`. BOTH intervals are always sent; `available` is the flag that says
 *  whether there is data, and `note` is the reason when there is not. Being listed is not being available. */
export type FuturesIntervalInfo={interval:string;available:boolean;note:string|null};
/** Which contract the candles belong to, so the panel can name it and its expiry. */
export type FuturesContract={tradingsymbol:string|null;instrument_token:number|null;expiry:string|null;
 days_to_expiry:number|null};
/** GET /api/derivatives/futures-chart?underlying=<SYMBOL>&interval=15m|1d (interval defaults to 15m).
 *  The front futures contract of one underlying, oldest candle first. */
export type FuturesChart=Envelope&{contract:FuturesContract|null;interval:string;candles:FuturesCandle[];
 /** How many of those `candles` are bars the store actually has: the list carries its own gaps, so its
  *  length over-reports and is never the number shown. */
 bars:number|null;
 sessions:number|null;session:string|null;
 /** The series is too short to read as a series; the server's own sentence comes with it. */
 short_history:boolean;short_history_text:string|null;
 /** Every interval, each with its own availability flag and its own reason, so a dead control is disabled
  *  and EXPLAINED rather than hidden. */
 intervals:FuturesIntervalInfo[];
 /** One sentence for the interval that was served. */
 note:string|null};
export type UnderlyingChoice={underlying:string;options:number;futures:number;lot_size:number|null;is_index:boolean};
export type ExpiryChoice={underlying:string;expiry:string;days_to_expiry:number|null;contracts:number};
export type FilterData=Envelope&{underlyings:UnderlyingChoice[];expiries:ExpiryChoice[];
 watchlists:{key:string;label:string}[];option_types:string[];index_underlyings:string[]};
export type Status=Envelope&{tables:string[];contracts:Record<string,number>;backfill_sessions:number|null;
 metrics_ready:boolean;index_underlyings:string[];
 /** F&O CAPTURE COVERAGE, separately from the page-wide chip. The global chip describes the cash price and
  *  pattern feeds and went on reading healthy straight through a derivative capture outage. */
 capture?:CaptureHealth|null};
/** What the chart is pointed at. A row click anywhere on the tab writes one of these. */
export type ChartTarget={underlying:string;instrumentToken?:number|null;label:string;detail:string};


// =================================================================================================================
// The five session blocks (screener · PCR · max pain · IV · futures build-up).
//
// These shapes are read off the SERVED responses, not off a provisional contract: server/kanida_pilot/derivatives.py
// is what they describe. Four things about them are worth stating here, because getting any of them wrong is silent.
//
//  1. `available` on EVERY derivative response is the envelope's boolean — "is the F&O store readable". It is never
//     the filter list. The screener's filter list is `available_filters`, also nested as `filters.available`.
//  2. `applied` is a list of OBJECTS, not of keys: {key, value, always, text}. `always:true` marks the §3 floors and
//     the reading, which are on for every query; `always:false` marks what the reader asked for.
//  3. Every `points` array sits on the store's own reading grid. A reading the underlying has no row for is still a
//     slot: every value null, `gap:true`, and `withheld:"no_reading"`. Nothing is closed up or carried forward.
//  4. Max-pain distance is the STRIKE MINUS SPOT, the store's own convention. Positive = the strike sits above spot.
// =================================================================================================================

/** The direction words are SERVED, so the tab never hardcodes them. `direction_words` maps up/down/flat/none onto
 *  the vocabulary this series uses, and `direction_labels` gives each key its reader-facing word. */
export type DirectionWords={up:string;down:string;flat:string;none:string};
export type DirectionLabels=Record<string,string>;
/** Every session series carries the same head: what it is of, over what session, and how it reads direction. */
export type SessionEnvelope=Envelope&{underlying:string;expiry:string;expiry_basis?:string|null;
 expiries?:string[]|null;session:string|null;
 /** Counted from `session` in whole calendar days, NEVER from today. The same 22 Sep option used to read as
  *  4 days on one panel and 2 on another because two panels counted from two different dates. */
 days_to_expiry:number|null;days_to_expiry_basis?:string|null;
 direction_text:string;direction_labels:DirectionLabels;direction_words:DirectionWords;
 gaps_text:string;direction_lookback_marks:number;flat_fraction:number;series_source?:string|null;
 total_readings:number|null;readings_with_value:number|null;
 /** reason key → how many readings it accounts for. Empty when every reading carried a value. */
 withheld_reasons?:Record<string,number>|null;
 /** reason key → the server's sentence for it. Always preferred over any wording of ours. */
 reason_text?:Record<string,string>|null;
 definition?:string|null;
 chain_floors?:Record<string,number>|null;chain_floors_text?:string|null};
/** One reading of any session series. `gap:true` means the store has no row here at all. */
export type SessionPointBase={at:string|null;gap?:boolean;withheld?:string|null};

/** GET /api/derivatives/pcr-series?underlying=<SYMBOL>[&expiry=] */
export type PcrPoint=SessionPointBase&{pcr_oi:number|null;pcr_volume:number|null;
 total_ce_oi:number|null;total_pe_oi:number|null;contracts:number|null};
export type PcrSeries=SessionEnvelope&{points:PcrPoint[];
 latest_pcr_oi:number|null;latest_pcr_volume:number|null;
 latest_total_ce_oi?:number|null;latest_total_pe_oi?:number|null;
 /** WHICH READING each `latest_*` came from. They are resolved one figure at a time, so two of them can be
  *  an hour apart - on a session rebuilt from candles the strike is current and the spot is not, because a
  *  candle carries no spot. The panel prints the reading beside any figure that is not the block's own. */
 latest_pcr_oi_at?:string|null;latest_pcr_volume_at?:string|null;
 latest_total_ce_oi_at?:string|null;latest_total_pe_oi_at?:string|null;
 /** What the OI ratio did across the session, and the server's own word for it. */
 direction:string;direction_label:string;direction_detail?:Record<string,unknown>|null;
 /** The volume ratio reads on its own, and gets its own chip rather than borrowing the OI one. */
 volume_direction:string;volume_direction_label:string;
 volume_direction_detail?:Record<string,unknown>|null};

/** GET /api/derivatives/maxpain-series?underlying=<SYMBOL>[&expiry=] — note the path: `maxpain`, not `max-pain`. */
export type MaxPainPoint=SessionPointBase&{max_pain_strike:number|null;spot:number|null;
 /** STRIKE MINUS SPOT. Positive = the strike is above spot. */
 distance:number|null;distance_computed?:boolean;total_oi:number|null;contracts:number|null;
 store_status?:string|null};
export type MaxPainSeries=SessionEnvelope&{points:MaxPainPoint[];
 latest_max_pain_strike:number|null;latest_spot:number|null;latest_distance:number|null;
 /** WHICH READING each `latest_*` came from. They are resolved one figure at a time, so two of them can be
  *  an hour apart - on a session rebuilt from candles the strike is current and the spot is not, because a
  *  candle carries no spot. The panel prints the reading beside any figure that is not the block's own. */
 latest_max_pain_strike_at?:string|null;latest_spot_at?:string|null;latest_distance_at?:string|null;
 latest_total_oi_at?:string|null;
 latest_total_oi:number|null;
 /** WHY THERE IS NO DISTANCE, when there is none. A distance is not a measurement - it is a RELATIONSHIP
  *  between two of them, so it exists only at a reading that carried BOTH. `latest_distance` is resolved at
  *  the headline STRIKE's own reading and nowhere else: the 11:30 pair's distance is never printed beside the
  *  15:45 strike, which is the sentence this tab used to make. */
 latest_distance_reason?:string|null;latest_distance_withheld?:string|null;
 distance_reason_text?:Record<string,string>|null;
 /** The newest reading that carried a strike AND a spot together, OFFERED with its own time beside the
  *  headline. It is never mixed into it and never substituted for it. */
 latest_complete_pair?:{at:string|null;max_pain_strike:number|null;spot:number|null;distance:number|null}|null;
 latest_complete_pair_text?:string|null;
 /** The server's own sentence for the sign convention. Shown, never paraphrased. */
 distance_definition:string;
 direction:string;direction_label:string;direction_detail?:Record<string,unknown>|null};

/** One solved (or refused) reading of one option's implied volatility. */
export type IvLegPoint=SessionPointBase&{iv:number|null;iv_pct:number|null;computed:boolean;
 reason:string|null;reason_text:string|null;
 last_price:number|null;spot:number|null;strike:number|null;
 years_to_expiry?:number|null;intrinsic?:number|null;time_value?:number|null;
 last_trade_at?:string|null;staleness?:string|null;seconds_since_last_trade?:number|null;
 /** What the solved volatility would be at a risk-free rate one percentage point higher, in percentage points.
  *  This is here because the rate itself is a code constant with no feed behind it — the weakest input in the
  *  whole computation — so the reader is given its weight rather than asked to trust it. */
 rate_sensitivity_pct_points:number|null};
/** The at-the-money reading: the call leg, the put leg, and the average of the two. */
export type IvLeg={present:boolean;strike:number|null;option_type:string;tradingsymbol:string|null;
 instrument_token:number|null;points:IvLegPoint[];direction:string;direction_label:string;
 rejections?:Record<string,number>|null;readings_with_value:number|null;missing_text?:string|null};
export type IvAtmPoint=SessionPointBase&{iv:number|null;iv_pct:number|null;computed:boolean;
 /** "call and put" / "call only" / "put only" / "neither" — what the average was taken over. */
 basis:string|null;ce_iv_pct:number|null;pe_iv_pct:number|null;
 ce_reason:string|null;pe_reason:string|null;
 reason:string|null;reason_text:string|null;spot:number|null};
export type IvAtm={strike:number|null;ce:IvLeg|null;pe:IvLeg|null;points:IvAtmPoint[];
 direction:string;direction_label:string;rejections?:Record<string,number>|null;
 basis_rule?:string|null;readings_with_value:number|null};
/** Where the risk-free rate came from. `live_feed:false` is the thing the reader must be able to find out. */
export type RiskFreeSource={kind:string;where:string;live_feed:boolean;text:string};
/** GET /api/derivatives/iv-series?underlying=<SYMBOL>[&expiry=][&strike=&option_type=]
 *
 *  THE ONE COMPUTED NUMBER ON THIS TAB. `computed` is true and `exchange_reported` is false; the model, the rate,
 *  where the rate came from and every refusal travel with it. */
export type IvSeries=SessionEnvelope&{computed:boolean;exchange_reported:boolean;
 model:string;method:string;day_count:string;expiry_time_ist:string;iv_bracket:number[];
 computed_text:string;atm_rule:string;
 risk_free_rate:number|null;risk_free_rate_source:RiskFreeSource;
 /** Every modelling assumption, in the server's own words, for the block's one disclosure. */
 assumptions:Record<string,string>;
 points:IvAtmPoint[];atm:IvAtm|null;atm_strike:number|null;spot:number|null;
 strike?:number|null;stale_seconds?:number|null;
 latest_iv:number|null;latest_iv_pct:number|null;
 /** WHICH READING each `latest_*` came from. They are resolved one figure at a time, so two of them can be
  *  an hour apart - on a session rebuilt from candles the strike is current and the spot is not, because a
  *  candle carries no spot. The panel prints the reading beside any figure that is not the block's own. */
 latest_iv_at?:string|null;latest_iv_pct_at?:string|null;
 /** reason key → how many readings it refused. */
 rejections?:Record<string,number>|null;
 direction:string;direction_label:string;direction_detail?:Record<string,unknown>|null};

/** GET /api/derivatives/futures-buildup?underlying=<SYMBOL> */
export type FuturesBuildupPoint=SessionPointBase&{oi:number|null;oi_avg:number|null;oi_vs_avg:number|null;
 basis:number|null;basis_pct:number|null;last_price:number|null;spot:number|null;
 /** Served as READER-FACING labels ("Long build-up", "no data"), not as the snake keys §3.1 uses elsewhere. */
 buildup_15m:string|null;buildup_day:string|null;
 oi_change_pct_15m:number|null;oi_change_pct_day:number|null;
 oi_status?:string|null;basis_status?:string|null};
export type FuturesBuildup=SessionEnvelope&{contract:FuturesContract|null;points:FuturesBuildupPoint[];
 latest_oi_vs_avg:number|null;latest_basis:number|null;latest_basis_pct:number|null;
 latest_oi?:number|null;latest_oi_change_pct_day?:number|null;
 /** WHICH READING each `latest_*` came from. They are resolved one figure at a time, so two of them can be
  *  an hour apart - on a session rebuilt from candles the strike is current and the spot is not, because a
  *  candle carries no spot. The panel prints the reading beside any figure that is not the block's own. */
 latest_oi_vs_avg_at?:string|null;latest_basis_at?:string|null;latest_basis_pct_at?:string|null;
 latest_oi_at?:string|null;latest_oi_change_pct_day_at?:string|null;
 latest_buildup_day_at?:string|null;latest_buildup_15m_at?:string|null;
 latest_buildup_15m:string|null;latest_buildup_day:string|null;
 /** Two readings, two chips: open interest builds or unwinds, and the basis widens or narrows. */
 oi_direction:string;oi_direction_label:string;oi_direction_detail?:Record<string,unknown>|null;
 basis_direction:string;basis_direction_label:string;basis_direction_detail?:Record<string,unknown>|null;
 buildup_labels?:Record<string,string>|null;empty_note:string|null};

/** One row of the screener. Every signal of §3.2-§3.4 on the row, plus where the strike sits and what kind of
 *  underlying it is. */
export type ScreenerRow=ContractRow&{underlying_kind:string|null;moneyness:string|null;
 /** The build-up over the window the query asked for, and which window that was — never the two mixed. */
 buildup:string|null;buildup_window:string|null;
 volume_baseline_status?:string|null;volume_to_oi_status?:string|null;
 /** What each ratio was measured AGAINST. A multiple with no denominator beside it cannot be judged. */
 volume_baseline_median?:number|null;volume_to_oi_prev_oi?:number|null;
 /** The store's own §3 flag, its own reason words, and the SAME flags as structured triggers. */
 unusual?:boolean;unusual_reasons?:string|null;unusual_triggers?:UnusualTrigger[]|null};

/** ONE RULE FIRING ON ONE CONTRACT AT ONE READING.
 *
 *  The store writes WHY a contract was flagged as prose — "volume 206.0x its own time-of-day median" — and
 *  that sentence differs at every multiple, so NIFTY's 80 flagged contracts at the 11:30 reading of
 *  18 Sep 2026 wrote 124 different sentences for TWO rules. A trigger is the rule and its numbers, never the
 *  sentence: which rule, at which version, what was measured, how it was compared, against what threshold,
 *  over which baseline, and how many observations that baseline stands on. */
export type UnusualTrigger={rule_id:string;rule_version?:number;
 /** The measured multiple. Null only when the store recorded the condition but carries no number for it. */
 value:number|null;comparator?:string|null;threshold?:number|null;
 baseline?:number|null;sample_count?:number|null;unit?:string|null;
 /** The store's own words, kept so a condition this build does not name is still readable. */
 text?:string|null};
/** One rule in the closed registry: what it measures, how it compares, and against what. No numbers from any
 *  contract live here — those are on the trigger and on the tally. */
export type UnusualRuleDef={rule_id:string;rule_version?:number;label:string;short_label?:string|null;
 measure?:string|null;baseline_label?:string|null;sample_label?:string|null;
 comparator?:string|null;threshold?:number|null;unit?:string|null;text?:string|null};
/** One rule, tallied across an instrument's flagged contracts. Counts of contracts and of firings — never a
 *  mean of multiples, so the largest reading travels with the contract it belongs to and its own baseline. */
export type UnusualRuleTally={rule_id:string;rule_version?:number;label:string;short_label?:string|null;
 measure?:string|null;baseline_label?:string|null;sample_label?:string|null;
 comparator?:string|null;threshold?:number|null;unit?:string|null;text?:string|null;
 contracts:number;observations:number;
 value_max?:number|null;value_max_symbol?:string|null;
 baseline_at_max?:number|null;sample_count_at_max?:number|null};
/** One flagged contract and every rule that fired on it — the evidence behind an instrument's badge. */
export type UnusualContract={tradingsymbol:string;instrument_type:string;strike:number|null;expiry:string;
 premium_cr:number|null;triggers:UnusualTrigger[]};
/** One key of the order the server ACTUALLY served, so the page prints the sort in force. */
export type RankingKey={field:string;direction:string;text:string};
export type Ranking={view:string;label:string;text:string;keys:RankingKey[]};
/** One filter this store can be asked for, and whether it can actually answer it right now. */
export type ScreenerFilter={key:string;kind:string;op:string;
 /** False when the store is missing a column the filter needs; `missing_columns` names them. */
 ready:boolean;text:string;requires_columns?:string[];missing_columns?:string[];
 min?:number;max?:number;unit?:string;values?:string[]};
/** One filter the query ACTUALLY applied. `always:true` is a §3 floor or the reading, on for every query. */
export type ScreenerApplied={key:string;value:unknown;always:boolean;text:string};
/** One 15-minute reading the store holds, and how wide it was. */
export type ScreenerReading={at:string;rows:number|null;underlyings:number|null};
/** ONE ROW PER UNDERLYING - the screener's default view, and the answer to "why other stocks are not
 *  populating": the contract list is sorted by premium, and one busy index owned every visible row of it.
 *
 *  Every number here is a SUM of things that add (rupees, contracts) or a COUNT of contracts. The two §3
 *  ratios are deliberately NOT averaged - a mean of ratios is not a ratio - so volume-against-median arrives
 *  as the largest reading among this name's contracts WITH THAT CONTRACT NAMED, and volume-to-open-interest
 *  arrives as a count over the tab's own existing threshold. The build-up label is a statement about one
 *  contract, so there is no underlying-level label at all, only counts. */
export type ScreenerGroup={underlying:string;underlying_kind:string|null;
 contracts:number;
 /** The options half and the futures half. The calls/puts split describes the OPTIONS: a future is neither,
  *  and a name with futures and no listed options must not read as "0C / 0P". */
 options:number;futures:number;calls:number;puts:number;
 premium_cr:number|null;volume:number|null;oi:number|null;
 oi_change_day:number|null;oi_change_15m:number|null;
 /** Not an aggregate: every contract of one underlying at one reading carries the same spot. */
 spot:number|null;expiries:string[];days_to_expiry:number|null;
 /** The LARGEST §3.2 ratio among this name's contracts, and the contract it belongs to. Never a mean. */
 volume_ratio_max:number|null;volume_ratio_max_symbol:string|null;volume_baseline_contracts:number;
 /** True when this name's contracts carried two different spots, so none is served as the instrument's. */
 spot_disagrees?:boolean;
 /** §3.3 as a count: contracts that traded more today than was standing at the previous close, and how many
  *  could carry the ratio at all. The store computes it for no futures contract, so a futures-only name gets
  *  null - never a zero, which would read as "nothing unusual" rather than "not measured". */
 volume_to_oi_over_1:number|null;volume_to_oi_contracts:number;
 /** WHAT IS UNUSUAL IN THIS NAME, AND IT IS THREE DIFFERENT NUMBERS, kept apart:
  *
  *   `unusual`              how many CONTRACTS the store flagged;
  *   `unusual_rule_count`   how many DISTINCT RULES they tripped between them;
  *   `unusual_observations` how many TIMES a rule fired across those contracts.
  *
  *  They used to be one: the server tallied the store's complete reason SENTENCE, which differs at every
  *  multiple, so 80 flagged NIFTY contracts produced 124 "conditions" and the tab clamped that to three.
  *  `unusual_rules` is the per-rule tally and `unusual_contracts` the evidence behind it.
  *  `unusual_reasons` is the older servers' sentence tally, normalised at the boundary in logic.ts. */
 unusual?:number;unusual_rule_count?:number|null;unusual_observations?:number|null;
 unusual_rules?:UnusualRuleTally[]|null;unusual_contracts?:UnusualContract[]|null;
 unusual_reasons?:Record<string,number>|null;
 /** §3.1 as counts per label. There is no underlying-level build-up LABEL and none is served. */
 buildup_counts:Record<string,number>;
 top:{tradingsymbol:string;strike:number|null;instrument_type:string;premium_cr:number|null;
  instrument_token:number|null;expiry:string;days_to_expiry:number|null}|null};
/** GET /api/derivatives/screener?<filters>
 *
 *  A filter this screener does not offer is a 400 that NAMES it, never a parameter quietly dropped while the panel
 *  goes on showing it as active. */
export type Screener=Envelope&{
 /** The rows of the VIEW being served: one per underlying by default, one per contract in the drill-down. */
 rows:ScreenerRow[]|ScreenerGroup[];
 view?:string;views?:string[];
 /** The order the server actually served these rows in, key by key. The page prints THIS, never a sort it
  *  assumed: the block below the screener used to take its instrument from row one of an unusual-ranked list
  *  and call it "Busiest by premium", which the ordering never guaranteed. */
 ranking?:Ranking|null;
 /** The closed list of rules a contract can be flagged under, and the version of their definitions. */
 unusual_rules?:UnusualRuleDef[]|null;unusual_rules_version?:number|null;
 /** How many underlyings have a contract over the floors, and how many contracts that is between them. */
 groups_total?:number;contracts_total?:number;
 /** What the query did. The ONLY thing that may make a filter render as active. */
 applied:ScreenerApplied[];
 /** What could have been asked for. NOT `available` — that is the envelope's boolean on this route too. */
 available_filters:ScreenerFilter[];
 filters:{applied:ScreenerApplied[];available:ScreenerFilter[]}|null;
 readings:ScreenerReading[];
 /** How wide the reading on screen was: its rows, and how many underlyings it covered. */
 coverage:ScreenerReading|null;
 /** `scanned` is every metric row at this reading; `total` what came through the floors and filters;
  *  `returned` how many of those this response carries. */
 scanned:number|null;total:number;returned:number|null;limit:number|null;scanned_text?:string|null;
 buildup_window:string;buildup_values?:string[]|null;buildup_labels?:Record<string,string>|null;
 moneyness_text?:string|null;index_underlyings?:string[]|null;
 // WHICH 15-MIN READING THIS IS. The newest reading the store holds is not always one that can be read: a
 // session rebuilt from 15-minute candles carries no traded-price average, so no contract in it has a premium
 // and none of them can clear the premium floor. The server opens on the newest reading that HAS rows over the
 // floors and serves these FACTS about the choice; the sentence is built in logic.readingNote.
 newest_at?:string|null;reading_at?:string|null;reading_is_newest?:boolean;reading_chosen?:boolean;
 reading_skipped?:number|null;reading_rule?:string|null;
 /** WHAT AN EMPTY LIST IS SAYING. `empty_note` is the capture state's own sentence, so an outage can never
  *  be printed as a quiet market and a genuinely complete reading with nothing eligible still says so. */
 empty_note:string|null;empty_state?:CaptureState|null};

// --- Δ SINCE THE PREVIOUS SESSION'S CLOSE ---------------------------------------------------------------------
// ΔOI already means one thing on this tab: the figure at this 15-min reading minus the same figure at the LAST
// reading of the session before. PCR, max pain and implied volatility now mean exactly that too, so a Δ anywhere
// on this tab is the same arithmetic against the same baseline.
//
// NO PREVIOUS CLOSE MEANS NO Δ. The server sends null with a reason, never a 0 and never "unchanged": a 0 in a Δ
// is a claim that the figure did not move, and an absent baseline makes no claim at all. These are APPENDED
// intersections rather than edits to the series types above, so nothing already on this tab shifts under them.
/** The Δ header every session series carries: what a Δ is here, what it is measured in, and its baseline. */
export type SeriesDelta={delta_text?:string|null;delta_unit?:string|null;delta_unit_text?:string|null;
 delta_reason_text?:Record<string,string>|null;
 /** The reading the baseline came from — the previous session's LAST 15-min reading. */
 previous_close_at?:string|null;
 /** Why there is no baseline, when there is none. Present ⇒ every Δ in this response is null for this reason. */
 previous_close_reason?:string|null;
 /** Set when the previous close existed but was REFUSED by the same floors a live reading is refused by. */
 previous_close_withheld?:string|null;
 /** How many 15-min readings of this session carry a Δ at all. */
 readings_with_delta?:number|null};
/** A Δ on one reading: the number, or null and the reason it is null. Never a 0 standing in for a missing one. */
export type PointDelta={delta_reason?:string|null};
/** PCR's Δ is a change in the RATIO — 0.93 to 0.96 is +0.03. Never a percentage of a ratio. */
export type PcrDeltaPoint={delta_pcr_oi?:number|null;delta_pcr_oi_reason?:string|null;
 delta_pcr_volume?:number|null;delta_pcr_volume_reason?:string|null};
export type PcrDeltas=SeriesDelta&{previous_close_pcr_oi?:number|null;previous_close_pcr_volume?:number|null;
 latest_delta_pcr_oi?:number|null;latest_delta_pcr_oi_at?:string|null;
 latest_delta_pcr_volume?:number|null;latest_delta_pcr_volume_at?:string|null;
 points?:(PcrPoint&PcrDeltaPoint)[]};
/** Max pain is a STRIKE, so its Δ is in strike points and moves in whole strike steps. Never a percentage. */
export type MaxPainDeltaPoint={delta_max_pain_strike?:number|null;delta_max_pain_strike_reason?:string|null};
export type MaxPainDeltas=SeriesDelta&{previous_close_max_pain_strike?:number|null;
 latest_delta_max_pain_strike?:number|null;latest_delta_max_pain_strike_at?:string|null;
 points?:(MaxPainPoint&MaxPainDeltaPoint)[]};
/** IV is already a percentage, so its Δ is in VOLATILITY POINTS: 11.1% to 9.0% is 2.1 points down. Never a
 *  percentage change of a percentage. */
export type IvDeltaPoint={delta_iv?:number|null;delta_iv_reason?:string|null;
 delta_iv_pct?:number|null;delta_iv_pct_reason?:string|null};
export type IvLegDeltas={previous_close_iv?:number|null;previous_close_iv_pct?:number|null;
 previous_close_at?:string|null;previous_close_reason?:string|null;
 latest_delta_iv_pct?:number|null;latest_delta_iv_pct_at?:string|null;
 readings_with_delta?:number|null;delta_unit?:string|null;delta_unit_text?:string|null;
 delta_text?:string|null};
/** The at-the-money baseline is built the way the at-the-money reading is, and says which legs it rests on. */
export type IvAtmDeltas=IvLegDeltas&{previous_close_basis?:string|null};
export type IvDeltas=SeriesDelta&{previous_close_iv?:number|null;previous_close_iv_pct?:number|null;
 previous_close_basis?:string|null;
 latest_delta_iv?:number|null;latest_delta_iv_at?:string|null;
 latest_delta_iv_pct?:number|null;latest_delta_iv_pct_at?:string|null};
