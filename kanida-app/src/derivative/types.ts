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
