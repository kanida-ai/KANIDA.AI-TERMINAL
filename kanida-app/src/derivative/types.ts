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
 expiries?:string[]|null;session:string|null;days_to_expiry:number|null;
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
 latest_total_oi:number|null;
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
 unusual?:boolean;unusual_reasons?:string|null};
/** One filter this store can be asked for, and whether it can actually answer it right now. */
export type ScreenerFilter={key:string;kind:string;op:string;
 /** False when the store is missing a column the filter needs; `missing_columns` names them. */
 ready:boolean;text:string;requires_columns?:string[];missing_columns?:string[];
 min?:number;max?:number;unit?:string;values?:string[]};
/** One filter the query ACTUALLY applied. `always:true` is a §3 floor or the reading, on for every query. */
export type ScreenerApplied={key:string;value:unknown;always:boolean;text:string};
/** One 15-minute reading the store holds, and how wide it was. */
export type ScreenerReading={at:string;rows:number|null;underlyings:number|null};
/** GET /api/derivatives/screener?<filters>
 *
 *  A filter this screener does not offer is a 400 that NAMES it, never a parameter quietly dropped while the panel
 *  goes on showing it as active. */
export type Screener=Envelope&{rows:ScreenerRow[];
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
 empty_note:string|null};
