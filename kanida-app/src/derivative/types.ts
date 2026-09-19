// Shapes the Derivative tab reads from the pilot (docs/DERIVATIVES_SPEC.md §3-§4, served by
// server/kanida_pilot/derivatives.py). Every number here was CAPTURED; nothing on this tab is a forecast (§5).
// A signal the metrics worker has not written yet arrives as null and is named in `missing` - it is rendered as
// "no baseline" or a dash, never as zero.
export type Buildup='long_buildup'|'short_buildup'|'short_covering'|'long_unwinding';
export type Floors={premium_cr:number;oi_lots:number;last_price:number};
/** Which source answered: D2's reader module, or the `metrics` rows it writes. */
export type DerivativeSource='metrics_module'|'store'|'none';
export type Envelope={available:boolean;captured:boolean;as_of:string|null;floors:Floors;floors_text:string;
 source:DerivativeSource;missing:string[];empty_text:string;empty_reason:string|null;baseline_sessions_required:number};
/** One contract at one 15-minute mark. */
export type ContractRow={instrument_token:number|null;tradingsymbol:string;underlying:string;instrument_type:string;
 strike:number|null;expiry:string;lot_size:number|null;days_to_expiry:number|null;captured_at:string|null;
 last_price:number|null;oi:number|null;oi_lots:number|null;volume:number|null;average_price:number|null;
 premium_cr:number|null;price_change_15m_pct:number|null;oi_change_15m:number|null;oi_change_15m_pct:number|null;
 buildup_15m:Buildup|string|null;price_change_day_pct:number|null;oi_change_day:number|null;
 oi_change_day_pct:number|null;buildup_day:Buildup|string|null;
 /** §3.2: served only with at least `baseline_sessions_required` sessions behind it, else null + 'none'. */
 volume_ratio:number|null;volume_baseline_sessions:number|null;volume_baseline:'ok'|'none';
 volume_to_oi:number|null;previous_oi:number|null;basis:number|null;oi_vs_20d_avg:number|null;
 /** The underlying's price at this mark, when the metrics worker writes one; null = not captured, shown as a dash. */
 spot:number|null};
export type UnusualGroup={underlying:string;premium_cr:number;strike_count:number;calls:number;puts:number;
 expiries:string[];days_to_expiry:number|null;oi_change_day:number|null;strikes:ContractRow[]};
export type Unusual=Envelope&{rows:UnusualGroup[];total:number;floor_premium_cr:number;empty_note:string|null};
export type ChainRow={strike:number;ce:ContractRow|null;pe:ContractRow|null};
export type Chain=Envelope&{rows:ChainRow[];underlying:string;expiry:string;spot:number|null;
 days_to_expiry:number|null;total:number};
export type StrikeOi={strike:number;ce_oi:number|null;pe_oi:number|null;ce_oi_change_day:number|null;
 pe_oi_change_day:number|null;ce_buildup_day:string|null;pe_buildup_day:string|null};
export type OiByStrike=Envelope&{rows:StrikeOi[];underlying:string;expiry:string;spot:number|null;
 max_pain_strike:number|null;max_pain_distance:number|null;total_ce_oi:number|null;total_pe_oi:number|null;
 days_to_expiry:number|null};
export type IndexPoint={captured_at:string|null;spot:number|null;pcr_oi:number|null;pcr_volume:number|null;
 total_ce_oi:number|null;total_pe_oi:number|null;max_pain_strike:number|null};
export type IndexRow={underlying:string;captured:boolean;captured_at:string|null;spot:number|null;
 pcr_oi:number|null;pcr_volume:number|null;max_pain_strike:number|null;max_pain_distance:number|null;
 total_ce_oi:number|null;total_pe_oi:number|null;expiry:string;days_to_expiry:number|null;series:IndexPoint[]};
export type Indices=Envelope&{rows:IndexRow[]};
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
 metrics_ready:boolean;index_underlyings:string[]};
/** What the chart is pointed at. A row click anywhere on the tab writes one of these. */
export type ChartTarget={underlying:string;instrumentToken?:number|null;label:string;detail:string};

// =================================================================================================================
// The five session blocks (screener · PCR · max pain · IV · futures build-up).
//
// These are the shapes the tab is BUILT AGAINST. The routes are owned by the server side and land separately; until
// they do the reads 404 and every panel degrades exactly as the rest of the tab does — an unavailable widget says
// so and draws no rows, which is not the same as nothing happening in the market (§5).
//
// Two rules run through every shape below and are the reason several fields exist at all:
//  1. A filter is ACTIVE only when the server says it applied it. `applied` is that statement. A filter the reader
//     asked for that is not in `applied` is NOT active, whatever the browser thinks it sent.
//  2. Implied volatility is the one number on this tab the exchange did not say. It carries `computed`, the model,
//     the rate it used and a sentence, and every null carries the reason it is null.
// =================================================================================================================

/** GET /api/derivatives/screener — the tab's workhorse list. One row per contract that passed the floors AND
 *  every filter the server was able to apply. */
export type ScreenerFilterReport={
 /** The query parameter, e.g. `min_premium_cr`. This is the key `applied`/`available` are written in. */
 key:string;
 /** The server's own words for it, when it sends them. */
 label?:string|null;
 /** What the server received. */
 value?:string|number|null;
 /** Why a filter the reader asked for was NOT applied — a thin chain, a signal with no baseline, an unknown
  *  parameter. Shown verbatim; the browser never invents one. */
 reason?:string|null};
export type Screener=Envelope&{rows:ContractRow[];total:number;
 /** The filters the server APPLIED to these rows. Anything not in here did not narrow this list. */
 applied:string[];
 /** The filters this server can apply at all. A parameter outside this list is one it does not know. */
 available:string[];
 /** Per-filter detail, including the reason an asked-for filter was not applied. */
 filters?:ScreenerFilterReport[]|null;
 /** The server's own sentence about what it did not apply, preferred over ours whenever it sends one. */
 not_applied_text?:string|null;
 /** How many rows the floors and the applied filters left, before any cap. */
 matched?:number|null;
 empty_note:string|null};

/** One 15-minute reading of a per-underlying series. Every block below draws one of these. */
export type SessionPoint={at:string|null};
/** GET /api/derivatives/pcr-series?underlying=<SYMBOL> */
export type PcrPoint=SessionPoint&{pcr_oi:number|null;pcr_volume:number|null;
 total_ce_oi:number|null;total_pe_oi:number|null};
export type PcrSeries=Envelope&{underlying:string;expiry:string;days_to_expiry:number|null;
 points:PcrPoint[];latest_pcr_oi:number|null;latest_pcr_volume:number|null;
 total_ce_oi:number|null;total_pe_oi:number|null;
 /** What the ratio DID across the session. Never what it is going to do (§5). */
 direction:'rising'|'flat'|'falling'|'no baseline'|string;
 direction_text?:string|null;note?:string|null;
 /** Set when the chain was too thin to divide honestly; the number is withheld and this says why. */
 withheld?:boolean;withheld_text?:string|null};

/** GET /api/derivatives/max-pain-series?underlying=<SYMBOL> */
export type MaxPainPoint=SessionPoint&{max_pain_strike:number|null;spot:number|null;gap:number|null;
 total_oi:number|null};
export type MaxPainSeries=Envelope&{underlying:string;expiry:string;days_to_expiry:number|null;
 points:MaxPainPoint[];latest_max_pain:number|null;latest_spot:number|null;latest_gap:number|null;
 total_oi:number|null;total_ce_oi:number|null;total_pe_oi:number|null;
 /** The owner's words: shifting_up · stable · shifting_down. */
 direction:'shifting_up'|'stable'|'shifting_down'|'no baseline'|string;
 direction_text?:string|null;note?:string|null;
 withheld?:boolean;withheld_text?:string|null};

/** Why a solver returned nothing for one reading. The reader is told which of these it was, always. */
export type IvReason='stale_trade'|'no_time_value'|'below_intrinsic'|'no_convergence'|'expiry_today'|string;
export type IvPoint=SessionPoint&{iv:number|null;spot:number|null;
 reason:IvReason|null;reason_text:string|null};
export type IvStrikeRow={strike:number|null;option_type:'CE'|'PE'|string;atm_offset:number|null;
 moneyness:string|null;tradingsymbol:string|null;instrument_token:number|null;
 iv:number|null;reason:IvReason|null;reason_text:string|null;last_price:number|null};
/** GET /api/derivatives/iv-series?underlying=<SYMBOL> — ATM IV through the session, and the latest per strike.
 *
 *  This is the ONE block on the tab whose number the exchange never said. `computed` is always true and the panel
 *  says so wherever an IV appears — not once in a footnote. */
export type IvSeries=Envelope&{underlying:string;expiry:string;days_to_expiry:number|null;
 computed:boolean;
 /** The pricing model that produced these numbers, e.g. "Black-Scholes-Merton". */
 model:string|null;
 /** The risk-free rate it was solved at, as a fraction, plus the server's own label for it. */
 rate:number|null;rate_label:string|null;
 /** The server's one sentence saying this is a model output, preferred over ours. */
 computed_text:string|null;
 points:IvPoint[];strikes:IvStrikeRow[];
 latest_iv:number|null;atm_strike:number|null;spot:number|null;
 /** The owner's words: expanding · stable · cooling. */
 direction:'expanding'|'stable'|'cooling'|'no baseline'|string;
 direction_text?:string|null;note?:string|null;
 withheld?:boolean;withheld_text?:string|null};

/** GET /api/derivatives/futures-buildup?underlying=<SYMBOL> */
export type FuturesBuildupPoint=SessionPoint&{oi:number|null;oi_vs_avg:number|null;price:number|null;
 spot:number|null;basis:number|null;basis_pct:number|null};
export type FuturesBuildup=Envelope&{underlying:string;contract:FuturesContract|null;
 expiry:string;days_to_expiry:number|null;points:FuturesBuildupPoint[];
 latest_oi:number|null;previous_oi:number|null;oi_change_day:number|null;
 /** §3.7: OI as a share of its own 20-day average, and how many sessions that average is built from. */
 oi_vs_20d_avg:number|null;avg_sessions:number|null;
 basis:number|null;basis_pct:number|null;price:number|null;spot:number|null;
 buildup_15m:Buildup|string|null;buildup_day:Buildup|string|null;
 /** The same three words the ΔOI tiles use: building · flat · unwinding. */
 direction:'building'|'flat'|'unwinding'|'no baseline'|string;
 direction_text?:string|null;note?:string|null;
 withheld?:boolean;withheld_text?:string|null};
