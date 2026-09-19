// Pure formatting and card wording for the Derivative tab (docs/DERIVATIVES_SPEC.md §3-§5). No React, no fetch:
// scripts/check-derivative.cjs evaluates this file on its own.
//
// Three rules decide every function here:
//  1. A number that was not captured is a DASH, never a zero (§5).
//  2. A ratio without at least `baseline_sessions_required` sessions behind it is the words "no baseline",
//     never a number (§3.2) - and the UI never has to remember that, because the server already sends
//     volume_ratio: null with volume_baseline: 'none' and this file re-checks both.
//  3. Nothing is phrased as a prediction. Every line describes a state and says when it was captured (§5).
import type {ChainRow,ContractRow,DirectionLabels,DirectionWords,Envelope,Floors,GridDirection,GridPoint,GridPriceDirection,GridSlot,IndexRow,
 OiGrid,SeriesPoint,StrikeOi,UnusualGroup} from './types';
export const DASH='—',MINUS='−',RUPEE='₹',TIMES='×';
/** The one empty-state sentence for the whole tab. Matches derivatives.EMPTY_TEXT on the server. */
export const EMPTY_TEXT='No F&O data captured yet — capture starts at the next 15-min reading';
export const NO_BASELINE='no baseline';
export const MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const num=(v:unknown):number|null=>typeof v==='number'&&Number.isFinite(v)?v:null;

// --- numbers -----------------------------------------------------------------------------------------------
/** "₹48.0 cr". A premium that was not captured is a dash. */
export function crore(v:unknown,places=1){const n=num(v);return n==null?DASH:`${RUPEE}${n.toFixed(places)} cr`;}
/** Indian grouping for a whole number of units (OI and volume are in units, §3). */
export function units(v:unknown){const n=num(v);return n==null?DASH:Math.round(n).toLocaleString('en-IN');}
/** Compact OI for a tight column: 3,750,000 → "37.5L", 12,000,000 → "1.2Cr". One decimal below 100, then none. */
export function compact(v:unknown){
 const n=num(v);if(n==null)return DASH;
 const a=Math.abs(n),sign=n<0?MINUS:'';
 const fit=(x:number)=>{const text=x>=100?x.toFixed(0):x.toFixed(1);return text.endsWith('.0')?text.slice(0,-2):text};
 if(a>=1e7)return `${sign}${fit(a/1e7)}Cr`;
 if(a>=1e5)return `${sign}${fit(a/1e5)}L`;
 if(a>=1e3)return `${sign}${fit(a/1e3)}k`;
 return `${sign}${Math.round(a)}`;
}
/** "+1.4%" / "−0.9%" / dash. */
export function signed(v:unknown,places=1){const n=num(v);return n==null?DASH:`${n>0?'+':n<0?MINUS:''}${Math.abs(n).toFixed(places)}%`;}
/** A signed count of contracts, for an OI change: "+4,20,000" / dash. */
export function signedUnits(v:unknown){const n=num(v);return n==null?DASH:`${n>0?'+':n<0?MINUS:''}${Math.abs(Math.round(n)).toLocaleString('en-IN')}`;}
export function price(v:unknown,places=2){const n=num(v);return n==null?DASH:`${RUPEE}${n.toLocaleString('en-IN',{minimumFractionDigits:places,maximumFractionDigits:places})}`;}
export function strike(v:unknown){const n=num(v);return n==null?DASH:Math.round(n).toLocaleString('en-IN');}
/** A ratio: "1.2×" / dash. Use volumeRatio() for the §3.2 one, which can say "no baseline". */
export function ratio(v:unknown,places=2){const n=num(v);return n==null?DASH:`${n.toFixed(places)}${TIMES}`;}

// --- time --------------------------------------------------------------------------------------------------
/** The store's marks are IST wall-clock strings ("2026-09-18 14:45"); they are read as written, never re-zoned. */
export function stamp(v?:string|null){
 const text=String(v||'').trim();const m=/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?/.exec(text);
 if(!m)return null;
 const [,y,mo,d,hh,mm]=m;const month=MONTHS[Number(mo)-1];if(!month)return null;
 return {date:`${Number(d)} ${month} ${y}`,time:hh!=null?`${hh}:${mm}`:'',iso:`${y}-${mo}-${d}`};
}
/** "As of 18 Sep 2026 · 14:45 IST" — every card carries its own (§4). Nothing captured ⇒ says so. */
export function asOfText(v?:string|null){
 const s=stamp(v);if(!s)return 'Not captured yet';
 return `As of ${s.date}${s.time?` · ${s.time} IST`:''}`;
}
/** "14:45" for a series axis label. */
export function clock(v?:string|null){const s=stamp(v);return s&&s.time?s.time:DASH;}
/** "8 days to expiry" / "expires today" / "expired". */
export function dteText(v:unknown){
 const n=num(v);if(n==null)return DASH;
 if(n<0)return 'expired';
 if(n===0)return 'expires today';
 return `${Math.round(n)} day${Math.round(n)===1?'':'s'} to expiry`;
}
export function expiryText(expiry?:string|null,dte?:number|null){
 const s=stamp(expiry);if(!s)return DASH;
 const tail=dteText(dte);
 return tail===DASH?s.date:`${s.date} · ${tail}`;
}

// --- §3.1 build-up -------------------------------------------------------------------------------------------
export const BUILDUP_LABELS:Record<string,string>={long_buildup:'Long build-up',short_buildup:'Short build-up',
 short_covering:'Short covering',long_unwinding:'Long unwinding'};
/** An unknown or absent label is a dash — a build-up is never guessed from price alone. */
export function buildupLabel(key:unknown){const k=String(key||'');return BUILDUP_LABELS[k]||DASH;}
/** Colour intent only. 'up' = new longs / shorts covering; 'down' = new shorts / longs leaving. */
export function buildupTone(key:unknown):'up'|'down'|'flat'{
 const k=String(key||'');
 if(k==='long_buildup'||k==='short_covering')return 'up';
 if(k==='short_buildup'||k==='long_unwinding')return 'down';
 return 'flat';
}
/** §3.1: the 15-minute and the day-on-day classification are NEVER merged into one label. */
export function buildupPair(row:Pick<ContractRow,'buildup_15m'|'buildup_day'>){
 return {fifteen:buildupLabel(row?.buildup_15m),day:buildupLabel(row?.buildup_day)};
}

// --- §3.2 / §3.3 the two volume readings -----------------------------------------------------------------------
/** "2.4× its 10-session median" or the words "no baseline". Never a ratio without its sessions. */
export function volumeRatio(row?:Pick<ContractRow,'volume_ratio'|'volume_baseline'|'volume_baseline_sessions'>|null,required=3){
 const n=num(row?.volume_ratio),sessions=num(row?.volume_baseline_sessions);
 if(row?.volume_baseline!=='ok'||n==null||sessions==null||sessions<required)return NO_BASELINE;
 return `${n.toFixed(1)}${TIMES} its ${Math.round(sessions)}-session median`;
}
/** Short form for a table cell: "2.4×" or "no baseline". */
export function volumeRatioShort(row?:Pick<ContractRow,'volume_ratio'|'volume_baseline'|'volume_baseline_sessions'>|null,required=3){
 const n=num(row?.volume_ratio),sessions=num(row?.volume_baseline_sessions);
 if(row?.volume_baseline!=='ok'||n==null||sessions==null||sessions<required)return NO_BASELINE;
 return `${n.toFixed(1)}${TIMES}`;
}
/** §3.3 with its raw numbers beside it, as the spec asks. Above 1.0 is flagged by the caller, not worded here. */
export function volumeToOi(row?:Pick<ContractRow,'volume_to_oi'|'volume'|'previous_oi'>|null){
 const n=num(row?.volume_to_oi);if(n==null)return DASH;
 const vol=num(row?.volume),prev=num(row?.previous_oi);
 const raw=vol!=null&&prev!=null?` (${units(vol)} traded vs ${units(prev)} standing)`:'';
 return `${n.toFixed(2)}${TIMES}${raw}`;
}
export const volumeToOiHot=(row?:Pick<ContractRow,'volume_to_oi'>|null)=>(num(row?.volume_to_oi)??0)>1;

// --- §3.5 / §3.6 index numbers ---------------------------------------------------------------------------------
export function pcrText(v:unknown){const n=num(v);return n==null?DASH:n.toFixed(2);}
/** "25,000 · spot 120 above" — the distance is stated, never a direction claim. */
export function maxPainText(row?:Pick<IndexRow,'max_pain_strike'|'max_pain_distance'>|null){
 const s=num(row?.max_pain_strike);if(s==null)return DASH;
 const d=num(row?.max_pain_distance);
 if(d==null)return strike(s);
 if(Math.round(d)===0)return `${strike(s)} · spot at max pain`;
 return `${strike(s)} · spot ${units(Math.abs(d))} ${d>0?'above':'below'}`;
}
/** §3.6 asks for the total OI the max pain was computed from; absent totals say so rather than showing 0. */
export function maxPainBasis(ce:unknown,pe:unknown){
 const c=num(ce),p=num(pe);
 if(c==null&&p==null)return 'Total OI behind it: not captured';
 return `From ${units((c||0)+(p||0))} contracts of open interest (CE ${compact(c)} · PE ${compact(p)})`;
}
/** §3.7: OI as a share of its own 20-day average. */
export function oiShareText(v:unknown){const n=num(v);return n==null?NO_BASELINE:`${n.toFixed(2)}${TIMES} its 20-day average`;}
export function basisText(v:unknown){const n=num(v);return n==null?DASH:`${n>0?'+':n<0?MINUS:''}${price(Math.abs(n))}`;}

// --- card state ------------------------------------------------------------------------------------------------
export type CardPhase='loading'|'error'|'empty'|'ready';
export type CardState={phase:CardPhase;text:string};
/** One rule for every card: loading → error → nothing captured → nothing passed the filters → rows. */
export function cardState(body:(Envelope&{rows?:unknown[];points?:unknown[];candles?:unknown[];empty_note?:string|null})|null,
 opts:{loading?:boolean;error?:string;emptyText?:string}={}):CardState{
 if(opts.error)return {phase:'error',text:opts.error};
 if(!body)return {phase:opts.loading===false?'empty':'loading',text:opts.loading===false?EMPTY_TEXT:'Reading the F&O store…'};
 if(!body.available||!body.captured)return {phase:'empty',text:body.empty_reason||body.empty_text||EMPTY_TEXT};
 // rows for a table, points for a series, candles for the futures chart — one rule, whatever the payload is called
 const rows=(body.rows||body.points||body.candles||[]) as unknown[];
 if(!rows.length)return {phase:'empty',text:body.empty_note||opts.emptyText||'Nothing clears the filters at this 15-min reading.'};
 return {phase:'ready',text:''};
}
/** The floors line every card shows (§4). The server sends the sentence; this is the fallback and the short form. */
export function floorsText(floors?:Floors|null,sentence?:string|null){
 if(sentence)return sentence;
 if(!floors)return '';
 return `Liquidity floors in force: premium traded ≥ ${RUPEE}${floors.premium_cr} cr, OI ≥ ${floors.oi_lots} lot, last price ≥ ${RUPEE}${floors.last_price}.`;
}
/** Names the §3 signals the metrics worker has not written yet, so a dash is never mistaken for a zero. */
export const MISSING_LABELS:Record<string,string>={premium_cr:'premium (₹ cr)',premium_inr:'premium',
 oi_change_15m:'OI change (15 min)',oi_change_15m_pct:'OI change % (15 min)',buildup_15m:'build-up (15 min)',
 oi_change_day:'OI change (day)',oi_change_day_pct:'OI change % (day)',buildup_day:'build-up (day)',
 volume_ratio:'volume vs its average',volume_baseline_sessions:'baseline sessions',volume_to_oi:'volume-to-OI',
 previous_oi:'previous close OI',basis:'basis',oi_vs_20d_avg:'OI vs 20-day average',days_to_expiry:'days to expiry',
 spot:'the underlying price',
 price_change_15m_pct:'price change (15 min)',price_change_day_pct:'price change (day)'};
export function missingText(missing?:string[]|null,shown:string[]=[]){
 const keys=(missing||[]).filter(k=>!shown.length||shown.includes(k));
 if(!keys.length)return '';
 const names=keys.map(k=>MISSING_LABELS[k]||k.replace(/_/g,' '));
 const head=names.slice(0,3).join(', '),rest=names.length>3?` and ${names.length-3} more`:'';
 return `Not captured yet: ${head}${rest}.`;
}
/** Where the rows came from, in plain words. */
export function sourceText(source?:string|null){
 if(source==='metrics_module')return 'Signals read from the derivatives metrics.';
 if(source==='store')return 'Signals read from the stored 15-min metrics.';
 return '';
}

// --- §3 roll-up wording ------------------------------------------------------------------------------------------
/** "RELIANCE — 3 call strikes unusually active, OI +12%, ₹48 cr traded" (§3, roll-up), built only from what is there. */
export function groupSummary(group:UnusualGroup){
 const parts:string[]=[];
 if(group.calls)parts.push(`${group.calls} call strike${group.calls===1?'':'s'}`);
 if(group.puts)parts.push(`${group.puts} put strike${group.puts===1?'':'s'}`);
 const strikes=parts.length?`${parts.join(' and ')} over the floors`:`${group.strike_count} strikes over the floors`;
 const oi=group.oi_change_day==null?'':` · OI ${signedUnits(group.oi_change_day)} today`;
 return `${strikes} · ${crore(group.premium_cr,group.premium_cr>=10?0:1)} traded${oi}`;
}
/** One strike's line under the roll-up. */
export function strikeSummary(row:ContractRow,required=3){
 const kind=row.instrument_type==='CE'?'call':row.instrument_type==='PE'?'put':row.instrument_type||'';
 return `${strike(row.strike)} ${kind} · ${crore(row.premium_cr)} · ${buildupLabel(row.buildup_day)} · volume ${volumeRatio(row,required)}`;
}

// --- linked panel geometry --------------------------------------------------------------------------------------
export type Scaled={lo:number;hi:number;points:({x:number;y:number;i:number}|null)[]};
/** Maps a captured series onto a box. A gap stays null: a mark that was not captured is never drawn through. */
export function scaleSeries(points:SeriesPoint[],key:'price'|'oi',width:number,height:number):Scaled|null{
 const values=points.map(p=>{const v=(p as any)?.[key];return typeof v==='number'&&Number.isFinite(v)?v:null});
 const real=values.filter((v):v is number=>v!=null);
 if(real.length<2||width<=0||height<=0)return null;
 const lo=Math.min(...real),hi=Math.max(...real),span=(hi-lo)||Math.abs(hi)||1;
 const step=points.length>1?width/(points.length-1):0;
 return {lo,hi,points:values.map((v,i)=>v==null?null:{x:i*step,y:height-((v-lo)/span)*height,i})};
}
/** SVG path for a scaled series; each run of captured marks is its own move-to, so gaps are visible as gaps. */
export function linePath(scaled:Scaled|null){
 if(!scaled)return '';
 let out='',pen=false;
 for(const p of scaled.points){
  if(!p){pen=false;continue}
  out+=`${pen?'L':'M'}${p.x.toFixed(2)},${p.y.toFixed(2)}`;pen=true;
 }
 return out;
}
/** The x of the strike nearest a price (spot / max pain marker on the OI-by-strike chart). None → null. */
export function nearestStrikeIndex(rows:StrikeOi[]|ChainRow[],value:unknown){
 const v=num(value);if(v==null||!rows?.length)return null;
 let best=0,bestGap=Infinity;
 rows.forEach((row,i)=>{const gap=Math.abs((row.strike??NaN)-v);if(Number.isFinite(gap)&&gap<bestGap){best=i;bestGap=gap}});
 return Number.isFinite(bestGap)?best:null;
}
/** Largest CE/PE OI across the strikes, for the bar chart's own scale. 0 → null (nothing to scale by). */
export function oiPeak(rows:StrikeOi[]){
 let peak=0;
 for(const row of rows||[]){for(const v of [row.ce_oi,row.pe_oi]){const n=num(v);if(n!=null&&n>peak)peak=n}}
 return peak>0?peak:null;
}

// --- filters ------------------------------------------------------------------------------------------------------
export type DerivativeFilters={watchlist:string;underlying:string;expiry:string;maxDte:number|null;
 optionType:''|'CE'|'PE';minPremiumCr:number|null};
export const DEFAULT_FILTERS:DerivativeFilters={watchlist:'all',underlying:'',expiry:'',maxDte:null,optionType:'',minPremiumCr:null};
/** Filters come back from localStorage, so they are re-typed before they can reach a query string. */
export function sanitizeFilters(stored:unknown):DerivativeFilters{
 const raw=(stored||{}) as Record<string,unknown>;
 const text=(v:unknown,cap=40)=>typeof v==='string'&&v.length<=cap?v:'';
 const count=(v:unknown,max:number)=>typeof v==='number'&&Number.isFinite(v)&&v>=0&&v<=max?v:null;
 const type=text(raw.optionType);
 return {watchlist:text(raw.watchlist,20)||DEFAULT_FILTERS.watchlist,
  underlying:/^[A-Z0-9&._-]{1,40}$/.test(text(raw.underlying))?text(raw.underlying):'',
  expiry:/^\d{4}-\d{2}-\d{2}$/.test(text(raw.expiry))?text(raw.expiry):'',
  maxDte:count(raw.maxDte,400),optionType:type==='CE'||type==='PE'?type:'',
  minPremiumCr:count(raw.minPremiumCr,100000)};
}
export const DTE_CHOICES=[{value:null,label:'Any expiry'},{value:0,label:'Expiry day only'},{value:7,label:'Within 7 days'},
 {value:30,label:'Within 30 days'}];
export const PREMIUM_CHOICES=[{value:null,label:'Floor (₹2 cr)'},{value:5,label:'₹5 cr and up'},
 {value:10,label:'₹10 cr and up'},{value:25,label:'₹25 cr and up'},{value:50,label:'₹50 cr and up'}];
/** The query string for /api/derivatives/unusual. Only set filters are sent, so the server's own defaults stand. */
export function unusualQuery(f:DerivativeFilters){
 const q:string[]=[];
 if(f.underlying)q.push(`underlying=${encodeURIComponent(f.underlying)}`);
 if(f.expiry)q.push(`expiry=${encodeURIComponent(f.expiry)}`);
 if(f.watchlist&&f.watchlist!=='all')q.push(`watchlist=${encodeURIComponent(f.watchlist)}`);
 if(f.optionType)q.push(`option_type=${f.optionType}`);
 if(f.maxDte!=null)q.push(`max_dte=${Math.max(0,Math.round(f.maxDte))}`);
 if(f.minPremiumCr!=null)q.push(`min_premium_cr=${f.minPremiumCr}`);
 return q.length?`?${q.join('&')}`:'';
}
/** One line describing the filters in force, for the card subtitles. */
export function filterText(f:DerivativeFilters,watchlistLabel=''){
 const parts:string[]=[];
 parts.push(f.underlying||watchlistLabel||'All underlyings');
 if(f.expiry){const s=stamp(f.expiry);if(s)parts.push(s.date)}
 if(f.maxDte!=null)parts.push(DTE_CHOICES.find(c=>c.value===f.maxDte)?.label||`Within ${f.maxDte} days`);
 if(f.optionType)parts.push(f.optionType==='CE'?'Calls only':'Puts only');
 if(f.minPremiumCr!=null)parts.push(`≥ ${RUPEE}${f.minPremiumCr} cr`);
 return parts.join(' · ');
}
/** Expiries for the chosen underlying, newest expiry last; every underlying's when none is chosen. */
export function expiriesFor(expiries:{underlying:string;expiry:string;days_to_expiry:number|null}[],underlying:string){
 const rows=(expiries||[]).filter(e=>!underlying||e.underlying===underlying);
 const seen=new Map<string,{expiry:string;days_to_expiry:number|null}>();
 for(const row of rows)if(!seen.has(row.expiry))seen.set(row.expiry,{expiry:row.expiry,days_to_expiry:row.days_to_expiry});
 return Array.from(seen.values()).sort((a,b)=>a.expiry<b.expiry?-1:1);
}

// --- the Customize filter builder (§4) ------------------------------------------------------------------------------
// TrendSpider's "Customize (N filters)…" popup is a list of Column · Operator · Value rows. Ours is the same shape
// with one hard constraint: EVERY rule must be expressible as a query parameter the pilot already validates
// (server/kanida_pilot/app.py::_derivative_query). There is no client-side row filtering on this tab, so the list on
// screen is always exactly the list the server returned — a screen that quietly filtered further would put one set of
// rows under another set's as-of line and floors.
export type FilterColumn='underlying'|'expiry'|'optionType'|'dte'|'premium'|'watchlist'
 |'volumeRatio'|'volumeToOi'|'oiChange15m'|'oiChangeDay'|'buildup'|'moneyness'|'market';
export type FilterOperator='is'|'is_not'|'gt'|'lt'|'in_watchlist';
export type FilterRule={column:FilterColumn;operator:FilterOperator;value:string};
export const OPERATOR_LABELS:Record<FilterOperator,string>={is:'is',is_not:'is not',gt:'greater than',
 lt:'less than',in_watchlist:'is in watch list'};
/** The columns the popup offers, each with ONLY the operators the server can answer, and the parameter it becomes.
 *
 *  `route` says WHICH list a column can narrow. The six the tab has always had are answered by every derivative
 *  route; the seven the screener block adds are answered by `/api/derivatives/screener` alone and are never sent
 *  anywhere else. `noScreener` marks a column the screener route has NO parameter for — it is never sent there,
 *  because that route answers an unknown parameter with a 400 that names it rather than dropping it.
 *
 *  Whatever the route says it takes, a rule is rendered as ACTIVE only when the screener's own `applied` list
 *  says it applied it. That is the whole point: this browser never decides that a filter worked. */
export const FILTER_COLUMNS:{key:FilterColumn;label:string;operators:FilterOperator[];param:string;unit?:string;
 route?:'screener';noScreener?:boolean;twin?:string}[]=[
 {key:'underlying',label:'Symbol',operators:['is'],param:'underlying'},
 {key:'expiry',label:'Exp. date',operators:['is'],param:'expiry'},
 {key:'optionType',label:'Type',operators:['is','is_not'],param:'option_type'},
 {key:'dte',label:'DTE',operators:['lt'],param:'max_dte',unit:'days'},
 {key:'premium',label:'Premium',operators:['gt'],param:'min_premium_cr',unit:'₹ cr'},
 // The screener has no watch-list parameter, so this rule narrows the other lists and is NEVER sent there.
 {key:'watchlist',label:'Watch list',operators:['in_watchlist'],param:'watchlist',noScreener:true},
 {key:'volumeRatio',label:'Volume vs median',operators:['gt'],param:'min_volume_ratio',unit:'× median',
  route:'screener'},
 {key:'volumeToOi',label:'Volume to OI',operators:['gt'],param:'min_volume_to_oi',unit:'×',
  route:'screener'},
 // Two-sided: "greater than" sends the min_ bound, "less than" the max_ twin. `param` is the bound a rule
 // sends by default and `twin` the other; screenerParam picks whichever the operator asks for.
 {key:'oiChange15m',label:'OI change (15 min)',operators:['gt','lt'],param:'min_oi_change_15m_pct',
  twin:'max_oi_change_15m_pct',unit:'%',route:'screener'},
 {key:'oiChangeDay',label:'OI change (day)',operators:['gt','lt'],param:'min_oi_change_day_pct',
  twin:'max_oi_change_day_pct',unit:'%',route:'screener'},
 {key:'buildup',label:'Build-up',operators:['is'],param:'buildup',route:'screener'},
 {key:'moneyness',label:'Moneyness',operators:['is'],param:'moneyness',route:'screener'},
 {key:'market',label:'Index or stock',operators:['is'],param:'underlying_kind',route:'screener'},
];
export const filterColumn=(key:unknown)=>FILTER_COLUMNS.find(c=>c.key===key)||null;
export const columnLabel=(key:unknown)=>filterColumn(key)?.label||String(key||'');
/** Day counts offered for "DTE less than N". Days-to-expiry is a whole number, so "< N" is exactly `max_dte = N-1`. */
export const DTE_VALUES=[1,3,7,15,31];
/** ₹ crore steps offered for "Premium greater than V". The §3 floor always applies underneath. */
export const PREMIUM_VALUES=[2,5,10,25,50];
/** §3.2: multiples of a contract's OWN median cumulative volume by this time of day. */
export const VOLUME_RATIO_VALUES=[1.5,2,3,5,10];
/** §3.3: day volume ÷ previous-day closing OI. 1.0 is the §3 flag — more traded today than the whole book. */
export const VOLUME_TO_OI_VALUES=[0.5,1,2,5];
/** Percentage steps for the two OI-change filters (15-minute and day-on-day). */
export const OI_CHANGE_VALUES=[1,2,5,10,25];
/** The build-up values the SCREENER takes. They are the reader-facing labels, not the snake keys §3.1 uses on the
 *  other routes — the screener's rows carry the label and it filters on that. Two of them have no §3.1 equivalent:
 *  "Flat" is a contract that moved neither way, and "no data" is one the metrics worker could not label at all.
 *  A contract the store cannot label is still a contract, and the reader is allowed to look for those. */
export const BUILDUP_VALUES=['Long build-up','Short build-up','Short covering','Long unwinding','Flat','no data'];
/** Which window `buildup` reads. The 15-minute and the day-on-day label are never mixed in one answer (§3.1). */
export const BUILDUP_WINDOWS=[{value:'15m',label:'Over the last 15 minutes'},
 {value:'day',label:'Since the previous close'}];
export const BUILDUP_WINDOW_DEFAULT='15m';
/** Where the strike sits against spot. Three buckets, no fourth. */
export const MONEYNESS_VALUES=[{value:'itm',label:'In the money'},{value:'atm',label:'At the money'},
 {value:'otm',label:'Out of the money'}];
/** An index chain and a single stock's chain behave nothing alike, so the screener can hold them apart. */
export const MARKET_VALUES=[{value:'index',label:'Index'},{value:'stock',label:'Stock'}];
const PCT_RULE=(v:string)=>/^\d{1,3}(\.\d{1,2})?$/.test(v)&&Number(v)>=0&&Number(v)<=999;
const RULE_VALUE:Record<FilterColumn,(v:string)=>boolean>={
 underlying:v=>/^[A-Z0-9&._-]{1,40}$/.test(v),
 expiry:v=>/^\d{4}-\d{2}-\d{2}$/.test(v),
 optionType:v=>v==='CE'||v==='PE',
 dte:v=>/^\d{1,3}$/.test(v)&&Number(v)>=1&&Number(v)<=400,
 premium:v=>/^\d{1,6}(\.\d{1,2})?$/.test(v)&&Number(v)>=0&&Number(v)<=100000,
 watchlist:v=>/^[a-z][a-z0-9_-]{0,19}$/.test(v),
 volumeRatio:v=>/^\d{1,3}(\.\d{1,2})?$/.test(v)&&Number(v)>0&&Number(v)<=999,
 volumeToOi:v=>/^\d{1,3}(\.\d{1,2})?$/.test(v)&&Number(v)>0&&Number(v)<=999,
 oiChange15m:PCT_RULE,
 oiChangeDay:PCT_RULE,
 buildup:v=>BUILDUP_VALUES.includes(v),
 moneyness:v=>MONEYNESS_VALUES.some(m=>m.value===v),
 market:v=>MARKET_VALUES.some(m=>m.value===v),
};
/** A rule set restored from localStorage is re-typed before it can reach a query string: a bad rule is dropped, a
 *  second rule on a column the server can only answer once is dropped, and the list is capped. */
export function sanitizeRules(stored:unknown):FilterRule[]{
 const raw=Array.isArray(stored)?stored:[];
 const out:FilterRule[]=[],seen=new Set<string>();
 for(const item of raw){
  const row=(item||{}) as Record<string,unknown>;
  const column=filterColumn(row.column);if(!column||seen.has(column.key))continue;
  const operator=row.operator as FilterOperator;if(!column.operators.includes(operator))continue;
  const value=typeof row.value==='string'?row.value.trim():'';
  if(!value||!RULE_VALUE[column.key](value))continue;
  seen.add(column.key);out.push({column:column.key,operator,value});
  if(out.length>=FILTER_COLUMNS.length)break;
 }
 return out;
}
/** Columns still free for a new rule (the server answers each parameter once), keeping the row's own column. */
export function freeColumns(rules:FilterRule[],own?:FilterColumn){
 const used=new Set((rules||[]).map(r=>r.column));
 return FILTER_COLUMNS.filter(c=>c.key===own||!used.has(c.key));
}
/** A new row takes the first free column and that column's first operator. Null when every column is already used. */
export function newRule(rules:FilterRule[]):FilterRule|null{
 const free=freeColumns(rules||[])[0];
 return free?{column:free.key,operator:free.operators[0],value:''}:null;
}
/** Moving a row to another column re-picks the operator — operators are per column — and clears the value. */
export function withColumn(rule:FilterRule,key:FilterColumn):FilterRule{
 const column=filterColumn(key);
 if(!column||column.key===rule.column)return rule;
 return {column:column.key,operator:column.operators[0],value:''};
}
/** The rules folded into the filter set the server already validates. An incomplete row contributes nothing. */
export function rulesToFilters(rules:FilterRule[]):DerivativeFilters{
 const out:DerivativeFilters={...DEFAULT_FILTERS};
 for(const rule of sanitizeRules(rules)){
  if(rule.column==='underlying')out.underlying=rule.value;
  else if(rule.column==='expiry')out.expiry=rule.value;
  // "is not CE" is the other type outright: the server serves exactly two option types, so the complement is exact.
  else if(rule.column==='optionType')out.optionType=(rule.operator==='is_not'?(rule.value==='CE'?'PE':'CE'):rule.value) as 'CE'|'PE';
  // "less than N days" is the server's inclusive max_dte = N-1; days-to-expiry is served as a whole number.
  else if(rule.column==='dte')out.maxDte=Math.max(0,Math.round(Number(rule.value))-1);
  // "greater than V" is the server's inclusive min_premium_cr = V + 0.01; premium is served rounded to paise.
  else if(rule.column==='premium')out.minPremiumCr=Math.round((Number(rule.value)+0.01)*100)/100;
  else if(rule.column==='watchlist')out.watchlist=rule.value;
 }
 return out;
}
/** "Customize (2 filters)…" — the live count in the widget header bar. */
export function customizeLabel(count:number){return count>0?`Customize (${count} filter${count===1?'':'s'})…`:'Customize…';}
/** "Symbol is RELIANCE" — one rule in words, for the screen reader and the applied-filters line. */
export function ruleText(rule:FilterRule,valueLabel?:string){
 const column=filterColumn(rule?.column);
 const unit=!valueLabel&&column?.unit?` ${column.unit}`:'';
 return `${columnLabel(rule?.column)} ${OPERATOR_LABELS[rule?.operator]||rule?.operator} ${valueLabel||rule?.value||''}${unit}`.trim();
}
/** Every rule in force, in one line. No rule says so plainly rather than leaving the line blank. */
export function rulesText(rules:FilterRule[],labels?:(rule:FilterRule)=>string){
 const clean=sanitizeRules(rules);
 if(!clean.length)return 'No filters — every contract over the liquidity floors';
 return clean.map(r=>ruleText(r,labels?.(r))).join(' · ');
}

// --- sortable columns -------------------------------------------------------------------------------------------------
export type SortDir='asc'|'desc';
export type SortState={key:string;dir:SortDir}|null;
/** The header cycle: first click largest-first, second smallest-first, third back to the server's own order. */
export function nextSort(current:SortState,key:string):SortState{
 if(!current||current.key!==key)return {key,dir:'desc'};
 return current.dir==='desc'?{key,dir:'asc'}:null;
}
/** Compares two captured values. A value that was NOT captured sinks to the bottom whichever way the column is
 *  sorted — a dash is not a small number and must never sort as one. */
export function compareValues(a:unknown,b:unknown,dir:SortDir){
 const rank=(v:unknown)=>v==null||v===''||(typeof v==='number'&&!Number.isFinite(v))?1:0;
 const ra=rank(a),rb=rank(b);
 if(ra||rb)return ra-rb;
 const sign=dir==='asc'?1:-1;
 if(typeof a==='number'&&typeof b==='number')return (a-b)*sign;
 const sa=String(a),sb=String(b);
 return (sa<sb?-1:sa>sb?1:0)*sign;
}
/** Sorts a COPY of the rows by one column's captured value, ties keeping the server's order. The rows are whatever
 *  the server returned: sorting re-orders that list and never re-queries it, so the as-of line and the floors
 *  printed under the table still describe exactly these rows. */
export function sortRows<R>(rows:R[],get:(row:R)=>unknown,dir:SortDir):R[]{
 return (rows||[]).map((row,i)=>({row,i}))
  .sort((a,b)=>compareValues(get(a.row),get(b.row),dir)||a.i-b.i)
  .map(x=>x.row);
}

// --- table wording ------------------------------------------------------------------------------------------------------
/** "25 Sep" for a tight expiry column. Nothing captured ⇒ empty, so a caller can leave the part out. */
export function shortDate(v?:string|null){
 const s=stamp(v);if(!s)return '';
 const [day,month]=s.date.split(' ');return `${day} ${month}`;
}
/** The "Summary" column: "25 Sep ₹25,000 CE". A part that was not captured is left out, never guessed. */
export function contractSummary(row?:Pick<ContractRow,'expiry'|'strike'|'instrument_type'>|null){
 const date=shortDate(row?.expiry);
 const level=num(row?.strike)==null?'':`${RUPEE}${strike(row?.strike)}`;
 const kind=String(row?.instrument_type||'').trim();
 const parts=[date,level,kind].filter(Boolean);
 return parts.length?parts.join(' '):DASH;
}
/** Colour intent for a contract type, so a call and a put read at a glance like the benchmark. */
export function optionTone(type:unknown):'call'|'put'|'flat'{
 const t=String(type||'').toUpperCase();
 return t==='CE'?'call':t==='PE'?'put':'flat';
}
/** The §3 roll-up groups flattened to one row per strike, for a flat table. Server order is preserved. */
export function flattenUnusual(groups?:UnusualGroup[]|null):ContractRow[]{
 const out:ContractRow[]=[];
 for(const group of groups||[])for(const row of group?.strikes||[])out.push(row);
 return out;
}
/** A row's identity in a table: two contracts never share one, and a row with no token still gets a stable key. */
export function rowKey(row:Pick<ContractRow,'instrument_token'|'tradingsymbol'|'strike'|'instrument_type'|'underlying'>|null,fallback:number|string=0){
 return String(row?.instrument_token??row?.tradingsymbol??`${row?.underlying}-${row?.strike}-${row?.instrument_type}-${fallback}`);
}

// --- the ΔOI strike grid (the owner's 2 × 5 block) ---------------------------------------------------------------
// Three definitions, stated here once and printed on the block itself:
//  * ΔOI is open interest added or removed SINCE THE PREVIOUS CLOSE. Every line starts at 0 at the first mark.
//  * ATM is the listed strike nearest spot in the front expiry; ATM+n is n strikes above, ATM−n n strikes below.
//  * Direction is the latest ΔOI against the mark four back (one hour), flat inside a 5% band of that contract's
//    own largest ΔOI today, and "no baseline" — no chip at all — under two marks.
// The wording is the owner's: BUILDING / FLAT / UNWINDING. Nothing here says bullish, bearish, or what happens next.
/** "now versus four readings ago (one hour)" — four 15-min readings. Matches DIRECTION_LOOKBACK_MARKS. */
export const GRID_DIRECTION_LOOKBACK=4;
/** |change| under this fraction of the contract's own largest |ΔOI| today is flat. Matches FLAT_FRACTION. */
export const GRID_FLAT_FRACTION=0.05;
/** The same 5% shape on the contract's OWN premium. Matches PRICE_FLAT_FRACTION on the server. */
export const GRID_PRICE_FLAT_FRACTION=0.05;
/** Neither side's total ΔOI change more than this many times the other's. Matches BLOCK_BALANCE_RATIO. */
export const GRID_BLOCK_BALANCE_RATIO=1.33;
/** The grid's empty sentence, word for word the server's NOT_ENOUGH_MARKS. */
export const GRID_NOT_ENOUGH_MARKS='Not enough readings captured yet — the first line appears after two 15-min readings';
/** A slot with no ΔOI at any mark. Not an error, and never a flat line drawn at zero. */
export const GRID_NO_DELTA='No ΔOI captured for this strike yet';
/** The owner's own chip wording. "no baseline" is deliberately absent: it never gets a chip. */
export const DIRECTION_CHIPS:Record<string,string>={building:'↑ BUILDING',flat:'→ FLAT',unwinding:'↓ UNWINDING'};
export const NO_DIRECTION='no baseline';

// --- price and ΔOI read together (the owner's two lines under every tile) --------------------------------------
// One table, keyed (option type | price direction | OI direction). The mechanics are the same for a call and a
// put - both are read on the option's OWN premium - and the wording differs only because writing a call and
// writing a put sit on opposite sides of the strike. Nothing here says what happens next, and nothing here is a
// recommendation: each row names who appears to be doing what, at the readings the tile draws.
//
// COMPLETE by construction: all nine (price x OI) combinations are listed for each option type, so a flat axis
// has its OWN row rather than being collapsed into the both-flat one. That collapse was a real bug on screen -
// a tile read "UNWINDING" over "Positioning is unchanged" while 3.7 million contracts closed. The rule the
// check script now enforces over the whole table: when OI has a direction the sentence says something happened
// to open interest; when OI is flat the sentence says open interest barely moved. Chip and sentence agree.
// Word for word FLOW_LABELS in server/kanida_pilot/derivatives.py and in market_data/derivatives/read_api.py;
// scripts/check-derivative.cjs reads all three and refuses a difference.
export const FLOW_LABELS:Record<string,[string,string]>={
 'CE|down|building':['Call writing increasing','Sellers are building resistance'],
 'CE|up|unwinding':['Call short covering','Call sellers are exiting'],
 'CE|up|building':['Call buying increasing','Traders are buying upside'],
 'CE|down|unwinding':['Call buyers exiting','Call buyers are closing out'],
 'CE|flat|building':['New positions added','Premium barely moved'],
 'CE|flat|unwinding':['Positions closing out','Premium barely moved'],
 'CE|up|flat':['Premium rose','Open interest barely moved'],
 'CE|down|flat':['Premium fell','Open interest barely moved'],
 'CE|flat|flat':['Very little change','Positioning is unchanged'],
 'PE|down|building':['Put writing increasing','Sellers are building support'],
 'PE|up|unwinding':['Put short covering','Put sellers are exiting'],
 'PE|up|building':['Put buying increasing','Traders are buying downside protection'],
 'PE|down|unwinding':['Put buyers exiting','Put buyers are closing out'],
 'PE|flat|building':['New positions added','Premium barely moved'],
 'PE|flat|unwinding':['Positions closing out','Premium barely moved'],
 'PE|up|flat':['Premium rose','Open interest barely moved'],
 'PE|down|flat':['Premium fell','Open interest barely moved'],
 'PE|flat|flat':['Very little change','Positioning is unchanged'],
};
/** ONLY when BOTH axes are flat. A tile whose OI moved never says this - it would contradict its own chip. */
export const FLOW_FLAT_WHAT='Very little change',FLOW_FLAT_MEANING='Positioning is unchanged';
/** Fewer than two readings carrying a price, or carrying a ΔOI. Never guessed at. */
export const FLOW_NOT_ENOUGH='Not enough readings yet';
/** The one line under the whole block, printed only when both sides are building and are of a similar size. */
export const GRID_BLOCK_BOTH_BUILDING='Both sides building similarly — no clear directional edge';
/** The legend: which line is which, said once under the block rather than crowded onto every tile. */
export const GRID_LEGEND_TEXT='Solid line: ΔOI, on the left scale. Dashed line: the contract\'s own last traded price, on its own scale.';

/** A signed ΔOI in the owner's units: "+2.4L", "−80K", "0". Not captured is a dash, never a zero. */
export function deltaUnits(v:unknown){
 const n=num(v);if(n==null)return DASH;
 const a=Math.abs(n),sign=n>0?'+':n<0?MINUS:'';
 const fit=(x:number)=>{const text=x>=100?x.toFixed(0):x.toFixed(1);return text.endsWith('.0')?text.slice(0,-2):text};
 if(a>=1e7)return `${sign}${fit(a/1e7)}Cr`;
 if(a>=1e5)return `${sign}${fit(a/1e5)}L`;
 if(a>=1e3)return `${sign}${fit(a/1e3)}K`;
 return `${sign}${Math.round(a)}`;
}
/** The four states a slot's direction may be in, derived from the very points the line is drawn from. */
export function gridDirection(points:GridPoint[]|null|undefined,lookback=GRID_DIRECTION_LOOKBACK,
 flatFraction=GRID_FLAT_FRACTION):GridDirection{
 const usable=(points||[]).map(p=>num(p?.delta_oi)).filter((v):v is number=>v!=null);
 if(usable.length<2)return NO_DIRECTION;
 const index=Math.max(0,usable.length-1-lookback);
 const change=usable[usable.length-1]-usable[index];
 const scale=Math.max(...usable.map(v=>Math.abs(v)));
 if(scale===0||Math.abs(change)<flatFraction*scale)return 'flat';
 return change>0?'building':'unwinding';
}
/** up / down / flat / "no baseline" on the contract's OWN premium, from the very points the tile draws. */
export function gridPriceDirection(points:GridPoint[]|null|undefined,lookback=GRID_DIRECTION_LOOKBACK,
 flatFraction=GRID_PRICE_FLAT_FRACTION):GridPriceDirection{
 const usable=(points||[]).map(p=>num(p?.price)).filter((v):v is number=>v!=null);
 if(usable.length<2)return NO_DIRECTION;
 const index=Math.max(0,usable.length-1-lookback);
 const change=usable[usable.length-1]-usable[index];
 // the band scales with the contract's own largest move from the day's first priced reading, as ΔOI's does
 const first=usable[0];
 const scale=Math.max(...usable.map(v=>Math.abs(v-first)));
 if(scale===0||Math.abs(change)<flatFraction*scale)return 'flat';
 return change>0?'up':'down';
}
/** What is happening on this contract, and what it means - derived from the tile's own points, never from a chip. */
export function gridFlow(optionType:unknown,points:GridPoint[]|null|undefined):
 {price_direction:GridPriceDirection;oi_direction:GridDirection;what_label:string;meaning:string}{
 const oi=gridDirection(points),price=gridPriceDirection(points);
 const kind=String(optionType||'').toUpperCase();
 if(price===NO_DIRECTION||oi===NO_DIRECTION)return {price_direction:price,oi_direction:oi,
  what_label:FLOW_NOT_ENOUGH,meaning:''};
 // a straight lookup: every combination has its own row, so nothing is collapsed into another one
 const found=FLOW_LABELS[`${kind}|${price}|${oi}`];
 return {price_direction:price,oi_direction:oi,what_label:found?found[0]:FLOW_NOT_ENOUGH,
  meaning:found?found[1]:''};
}
/** The window ΔOI change one tile contributes to the block read: now against the reading `lookback` back. */
export function gridDeltaChange(points:GridPoint[]|null|undefined,lookback=GRID_DIRECTION_LOOKBACK){
 const usable=(points||[]).map(p=>num(p?.delta_oi)).filter((v):v is number=>v!=null);
 if(usable.length<2)return null;
 return usable[usable.length-1]-usable[Math.max(0,usable.length-1-lookback)];
}
// The one line under the whole 2 x 5 block, or the empty string - aggregated from the ten slots already on
// screen. No second query and no strike the grid is not drawing: this sums the SAME window change each tile
// carries. Both rows building, and neither side total more than GRID_BLOCK_BALANCE_RATIO times the other, is
// the only thing it will say; anything else prints nothing rather than forcing a summary.
export function gridBlockRead(rows?:GridSlot[]|null,ratio=GRID_BLOCK_BALANCE_RATIO){
 let calls=0,puts=0,nc=0,np=0;
 for(const slot of rows||[]){
  const change=gridDeltaChange(slot?.points);
  if(change==null)continue;
  if(slot.row==='puts'){puts+=change;np++}else{calls+=change;nc++}
 }
 if(!nc||!np||calls<=0||puts<=0)return '';
 const high=Math.max(calls,puts),low=Math.min(calls,puts);
 if(low<=0||high>ratio*low)return '';
 return GRID_BLOCK_BOTH_BUILDING;
}
/** What a screen reader hears about the second line: the premium, and the reading of the two together. */
export function gridPriceSpoken(slot?:GridSlot|null){
 if(!slot)return '';
 const flow=gridFlow(slot.option_type,slot.points);
 const last=[...(slot.points||[])].reverse().find(p=>num(p?.price)!=null);
 const premium=last?`Price ${price(last.price)}.`:'No price captured for this strike yet.';
 return `${premium} ${flow.what_label}${flow.meaning?`. ${flow.meaning}`:''}.`;
}
/** The chip's text, or '' when there is no baseline for one. */
export function directionChip(direction:unknown){return DIRECTION_CHIPS[String(direction||'')]||'';}
/** Green for building, red for unwinding, neutral for flat and for no baseline. */
export function directionTone(direction:unknown):'up'|'down'|'flat'{
 const key=String(direction||'');
 return key==='building'?'up':key==='unwinding'?'down':'flat';
}
/** "25,000 CE" — the strike the chart is of. An unlisted slot falls back to its ATM label. */
export function gridSlotTitle(slot?:Pick<GridSlot,'strike'|'option_type'|'label'>|null){
 if(!slot)return DASH;
 const text=strike(slot.strike);
 return text===DASH?(slot.label||DASH):`${text} ${slot.option_type}`;
}
/** "ATM+2 CE · 12 readings" — where the slot sits and how much of the session it actually has. */
export function gridSlotDetail(slot?:GridSlot|null){
 if(!slot)return '';
 if(!slot.present)return slot.label;
 const marks=slot.marks_with_delta??0;
 return `${slot.label} · ${marks} reading${marks===1?'':'s'}`;
}
/** The one sentence a small chart shows instead of a line, or '' when it has something to draw. */
export function gridSlotNote(slot?:GridSlot|null){
 if(!slot)return GRID_NO_DELTA;
 if(!slot.present)return slot.missing_text||'This strike is not listed in this expiry.';
 if(!(slot.marks_with_delta??0))return GRID_NO_DELTA;
 return '';
}
/** What a screen reader hears for one small chart. Describes the captured state; never what comes next. */
export function gridSlotLabel(slot?:GridSlot|null,direction?:unknown){
 if(!slot)return '';
 if(!slot.present)return gridSlotNote(slot);
 const key=String(direction||slot.direction||'');
 const chip=DIRECTION_CHIPS[key];
 const state=chip?chip.replace(/^[^ ]+ /,'').toLowerCase():NO_DIRECTION;
 return `${gridSlotTitle(slot)}, ${slot.label}. Open interest ${deltaUnits(slot.latest_delta_oi)} against the `+
  `previous close over ${slot.marks_with_delta} captured reading${slot.marks_with_delta===1?'':'s'}. ${state}.`+
  ` ${gridPriceSpoken(slot)}`;
}
export type ScaledDelta={lo:number;hi:number;height:number;zero:number;real:number;
 points:({x:number;y:number;i:number}|null)[]};
/** Maps a ΔOI series onto a box with zero always in view. A mark without a ΔOI stays null, so a gap is a gap. */
export function scaleDelta(points:GridPoint[]|null|undefined,width:number,height:number):ScaledDelta|null{
 const values=(points||[]).map(p=>num(p?.delta_oi));
 const real=values.filter((v):v is number=>v!=null);
 if(!real.length||width<=0||height<=0)return null;
 // Zero is the baseline of the definition, so it is always on the axis — a line that never left zero must look flat.
 const lo=Math.min(0,...real),hi=Math.max(0,...real),span=(hi-lo)||1;
 const y=(v:number)=>height-((v-lo)/span)*height;
 const step=values.length>1?width/(values.length-1):0;
 return {lo,hi,height,zero:y(0),real:real.length,
  points:values.map((v,i)=>v==null?null:{x:i*step,y:y(v),i})};
}
/** Maps the price line onto the SAME box on its own scale. A reading without a price stays null: a gap is a gap. */
export function scaleGridPrice(points:GridPoint[]|null|undefined,width:number,height:number):Scaled|null{
 const values=(points||[]).map(p=>num(p?.price));
 const real=values.filter((v):v is number=>v!=null);
 if(real.length<2||width<=0||height<=0)return null;
 // NOT forced through zero: ΔOI is measured from a baseline, a premium is not, and squashing a ₹120 option
 // against a zero floor would hide every move it made.
 const lo=Math.min(...real),hi=Math.max(...real),span=(hi-lo)||Math.abs(hi)||1;
 const step=values.length>1?width/(values.length-1):0;
 return {lo,hi,points:values.map((v,i)=>v==null?null:{x:i*step,y:height-((v-lo)/span)*height,i})};
}
/** The y labels: the largest ΔOI, zero, and the smallest — in K/L, and only where they do not collide. */
export function deltaAxis(scaled:ScaledDelta|null){
 if(!scaled)return [] as {v:number;y:number}[];
 const span=(scaled.hi-scaled.lo)||1;
 const out:{v:number;y:number}[]=[];
 for(const v of [scaled.hi,0,scaled.lo]){
  const y=scaled.height-((v-scaled.lo)/span)*scaled.height;
  if(!out.some(t=>Math.abs(t.y-y)<9))out.push({v,y});
 }
 return out;
}
/** A few x labels taken from marks that were actually captured — the hour marks when they are there. */
export function axisTimes(points:GridPoint[]|null|undefined,want=4){
 const marks=(points||[]).map((p,i)=>({i,label:clock(p?.at)})).filter(m=>m.label!==DASH);
 if(!marks.length)return [] as {i:number;label:string}[];
 // 09:30, 10:30, 11:30 … when the capture has them; otherwise evenly spaced over the marks it does have.
 const hourly=marks.filter(m=>m.label.endsWith(':30'));
 const pool=hourly.length>=2?hourly:marks;
 if(pool.length<=want)return pool;
 const step=(pool.length-1)/(want-1);
 const picked:{i:number;label:string}[]=[];
 for(let k=0;k<want;k++){
  const mark=pool[Math.round(k*step)];
  if(mark&&!picked.some(m=>m.i===mark.i))picked.push(mark);
 }
 return picked;
}
/** The block's own footer line: where the money is and how much of the session is behind the lines. */
export function gridBasis(body?:OiGrid|null){
 if(!body)return '';
 const at=body.atm_strike==null?DASH:strike(body.atm_strike);
 const spot=body.spot==null?DASH:price(body.spot);
 const marks=(body.marks||[]).length;
 const window=marks?` ${marks} reading${marks===1?'':'s'} captured, ${clock(body.marks[0])} to ${clock(body.marks[marks-1])}.`:'';
 // the expiry is written the way every other date on this tab is written; an unreadable one is left exactly
 // as it was served rather than dropped, because it is still the expiry the numbers came from
 const when=body.expiry?(stamp(body.expiry)?.date||body.expiry):'';
 return `At the money: ${at}, from a captured spot of ${spot}${when?` in the ${when} expiry`:''}.${window}`;
}
/** Which table the readings came from, in plain words. Never dressed up as more than it is. */
export function gridSourceText(source?:string|null){
 if(source==='snapshots')return 'Readings taken from the 15-min capture.';
 if(source==='candles_15m')return 'Readings taken from the 15-min candle backfill; the live capture has not written this session yet.';
 if(source==='read_api')return 'Readings taken from the derivatives reader.';
 return '';
}

// --- the futures chart in the owner's ΔOI block ------------------------------------------------------------
// The middle panel of the block: the front futures contract of the block's ONE resolved symbol, drawn as
// candles, 15-min by default with the daily view one click away. Three rules, the same three as everywhere
// else on this tab:
//  * A candle is drawn only where one was STORED. A stored candle missing any of its four prices is left out
//    as a blank slot — never carried forward, never interpolated, never joined through.
//  * Nothing here is a reading of what comes next. The panel names the contract, the interval and how many
//    candles it drew, and stops there.
//  * An interval the server says has no data is DISABLED with its reason on screen, never quietly hidden.
/** The two intervals the panel offers. The block is captured at 15 minutes, so that is what opens. */
export const FUTURES_INTERVALS=[{key:'15m',label:'15-min candles'},{key:'1d',label:'Daily candles'}] as const;
export type FuturesInterval=(typeof FUTURES_INTERVALS)[number]['key'];
/** 15-min on arrival, word for word the endpoint's own default. */
export const FUTURES_DEFAULT_INTERVAL:FuturesInterval='15m';
/** Before the block has resolved a symbol there is nothing to draw, and the panel says so plainly. */
export const FUTURES_NO_SYMBOL='Choose a symbol in Customize, or click a row in the screener, to see its front futures contract.';
export const FUTURES_NO_CANDLES='No candles stored for this futures contract yet';
/** The fallback when the server flags a short series but sends no sentence of its own. */
export const FUTURES_SHORT_HISTORY='Too few candles stored to read this as a series.';
export const FUTURES_GAP_TEXT='Every candle drawn was stored. A reading with no candle is left blank — nothing is interpolated and no candle is carried forward.';
/** §5 on the panel itself: it describes what was stored, and says so. */
export const FUTURES_DISPLAY_TEXT='This panel draws what was stored for the contract named above. Nothing on it says what comes next.';
/** Under this many drawn candles the panel draws every one it has and says how few that is. */
export const FUTURES_FEW_CANDLES=3;
/** One sentence per interval, used only when the server sends none of its own. */
export const FUTURES_INTERVAL_NOTE:Record<string,string>={
 '15m':'Each candle is one 15-min reading of the front futures contract, oldest on the left.',
 '1d':'Each candle is one session of the front futures contract, oldest on the left.'};
export type FuturesIntervalChoice={key:FuturesInterval;label:string;available:boolean;reason:string};
/** The control at the top of the panel.
 *
 *  The endpoint sends BOTH intervals every time, each carrying its own `available` flag and its own `note`.
 *  That is the point of the shape: the note is the honest reason a control is dead ("daily history is fetched
 *  contract by contract, and this one has not been fetched"), and it would be thrown away if the server had to
 *  signal deadness by dropping the entry. So availability is read off the FLAG — being listed is not being
 *  available, and an entry flagged false is offered disabled with the server's own sentence beside it.
 *
 *  The shapes that are not that flag: a bare string, or an entry with no flag at all, is a listing and means
 *  live; an interval the server did not send at all is disabled with our own sentence; and an `intervals` that
 *  has not arrived yet is no claim about anything, so both choices stay live until the server actually speaks. */
export function futuresIntervalChoices(body?:{intervals?:unknown}|null):FuturesIntervalChoice[]{
 const raw=Array.isArray(body?.intervals)?(body as any).intervals as unknown[]:null;
 const served=raw?new Map<string,{available:boolean;note:string}>():null;
 for(const item of raw||[]){
  if(typeof item==='string'){if(item)served!.set(item,{available:true,note:''});continue}
  const row=(item||{}) as Record<string,unknown>;
  const key=String(row.interval||'');
  if(!key)continue;
  // only an explicit false is a claim of deadness; a missing flag is a listing, not a verdict
  served!.set(key,{available:row.available!==false,note:String(row.note||'').trim()});
 }
 return FUTURES_INTERVALS.map(c=>{
  const entry=served?served.get(c.key):null;
  // an interval the server did not send at all, when it sent a list, is not offered as live
  const available=served?!!entry?.available:true;
  if(available)return {key:c.key,label:c.label,available:true,reason:''};
  // the server's own reason first: it knows WHY, and a disabled control without a reason reads as broken
  return {key:c.key,label:c.label,available:false,
   reason:entry?.note||`No ${c.label.toLowerCase()} stored for this contract yet.`};
 });
}
/** The line under the control: every interval the server says is empty, named. '' when both are live. */
export function futuresIntervalNote(choices?:FuturesIntervalChoice[]|null){
 return (choices||[]).filter(c=>!c.available).map(c=>c.reason).join(' ');
}
export type FuturesContractRef={tradingsymbol?:string|null;instrument_token?:number|null;expiry?:string|null;
 days_to_expiry?:number|null};
/** "NIFTY25SEPFUT · 25 Sep 2026" — the panel names the CONTRACT it drew and when it expires, so the reader is
 *  never left guessing which of an underlying's contracts is on screen. */
export function futuresChartName(contract?:FuturesContractRef|null,symbol?:string|null){
 const sym=String(contract?.tradingsymbol||'').trim();
 const who=sym||(String(symbol||'').trim()?`${String(symbol).trim()} futures`:'');
 if(!who)return 'Futures chart';
 const when=stamp(contract?.expiry)?.date||'';
 return when?`${who} · ${when}`:who;
}
/** How many bars the store actually HAS. `candles` carries its own gaps — a gap arrives with its stamp set,
 *  every price null and `gap: true`, so it holds its place on the axis — and the length of that list therefore
 *  over-reports. The server's own `bars` is the count; without it, the gaps are dropped and what is left is
 *  counted by the same well-formed test the chart draws by, so the two can never disagree. */
export function futuresBars(body?:{bars?:unknown;candles?:Candle[]|null}|null){
 const served=num(body?.bars);
 if(served!=null)return Math.max(0,Math.round(served));
 return (body?.candles||[]).filter(c=>!(c as any)?.gap&&storedCandle(c)!=null).length;
}
/** The panel's HEADER title: the contract and nothing else, so it is never cut short in a narrow panel. The
 *  expiry moved to the subtitle beside it and the interval is named by the control that chose it — the same
 *  facts, laid out so each one fits. futuresChartName keeps the long form for what a screen reader hears. */
export function futuresChartTitle(contract?:FuturesContractRef|null,symbol?:string|null){
 const sym=String(contract?.tradingsymbol||'').trim();
 if(sym)return sym;
 const who=String(symbol||'').trim();
 return who?`${who} futures`:'Futures chart';
}
/** "25 Sep 2026 · 8 days to expiry" — when the contract named in the title expires, and how far that is. The
 *  interval is named by the control that chose it and the candle count sits under the chart, so neither is
 *  repeated here. A part that was not served is left out rather than guessed. */
export function futuresChartSubtitle(body?:{contract?:FuturesContractRef|null;bars?:unknown;
 candles?:Candle[]|null}|null){
 const parts:string[]=[];
 const when=stamp(body?.contract?.expiry)?.date||'';
 if(when)parts.push(when);
 const dte=dteText(body?.contract?.days_to_expiry);
 if(dte!==DASH)parts.push(dte);
 return parts.join(' · ');
}
/** The interval's own sentence: the server's when it sent one, ours when it did not. */
export function futuresNote(body?:{note?:string|null}|null,intervalKey:string=FUTURES_DEFAULT_INTERVAL){
 const served=String(body?.note||'').trim();
 return served||FUTURES_INTERVAL_NOTE[String(intervalKey)]||'';
}
/** The server's short-history sentence, or ours, and '' when the series is not short. */
export function futuresShortText(body?:{short_history?:boolean;short_history_text?:string|null}|null){
 if(!body?.short_history)return '';
 return String(body?.short_history_text||'').trim()||FUTURES_SHORT_HISTORY;
}
/** Fewer candles than it takes to read a series: draw every one that exists and say how few there are. */
export function futuresFewText(drawn:unknown,few=FUTURES_FEW_CANDLES){
 const n=num(drawn);
 if(n==null||n<=0||n>=few)return '';
 return `Only ${Math.round(n)} candle${Math.round(n)===1?'':'s'} stored for this contract — every one of them is drawn.`;
}
/** A gap arrives IN the series: its stamp is set, every price is null and `gap` is true. It holds its place
 *  on the axis so the hole is visible, and it is neither drawn nor counted as a bar the store has. */
export type Candle={at?:string|null;open?:number|null;high?:number|null;low?:number|null;close?:number|null;
 volume?:number|null;oi?:number|null;gap?:boolean};
export type ScaledCandle={i:number;x:number;open:number;close:number;high:number;low:number;
 direction:'up'|'down'|'flat'};
export type ScaledCandles={lo:number;hi:number;step:number;body:number;drawn:number;
 candles:(ScaledCandle|null)[]};
/** One candle's four prices, or null when it is not a candle that can be drawn: any price missing (which is
 *  what a `gap: true` slot looks like), a high under its own body, or a low over it. ONE test, used both to
 *  draw and to count, so the number on the subtitle and the bars on the chart can never disagree. */
export function storedCandle(candle?:Candle|null){
 const o=num(candle?.open),hi=num(candle?.high),lo=num(candle?.low),cl=num(candle?.close);
 if(o==null||hi==null||lo==null||cl==null)return null;
 if(hi<lo||hi<Math.max(o,cl)||lo>Math.min(o,cl))return null;
 return {o,hi,lo,c:cl};
}
/** Maps stored candles onto a box, oldest on the left. A candle that is not well formed — including a gap
 *  slot, which arrives with its stamp set and every price null — KEEPS ITS SLOT as a null and is simply not
 *  drawn. Nothing is carried forward into it and no line is drawn through it. */
export function scaleCandles(candles?:Candle[]|null,width?:number,height?:number):ScaledCandles|null{
 const w=num(width)||0,h=num(height)||0;
 const rows=(candles||[]).map(c=>storedCandle(c));
 const real=rows.filter((r):r is {o:number;hi:number;lo:number;c:number}=>r!=null);
 if(!real.length||w<=0||h<=0)return null;
 const low=Math.min(...real.map(r=>r.lo)),high=Math.max(...real.map(r=>r.hi));
 const span=(high-low)||Math.abs(high)||1;
 const y=(v:number)=>h-((v-low)/span)*h;
 const step=rows.length?w/rows.length:0;
 const body=Math.max(1,Math.min(11,step*0.62));
 return {lo:low,hi:high,step,body,drawn:real.length,
  candles:rows.map((r,i)=>r==null?null:{i,x:step*(i+0.5),open:y(r.o),close:y(r.c),high:y(r.hi),low:y(r.lo),
   direction:r.c>r.o?'up':r.c<r.o?'down':'flat'})};
}
/** A candle needs this much width before it is a candle rather than a smudge. 364 fifteen-minute bars in a
 *  250px panel is half a pixel each: every one is drawn, and not one of them is readable. */
export const CANDLE_MIN_STEP=3;
export type CandleWindow={candles:Candle[];shown:number;total:number;windowed:boolean};
/** The most recent candles that fit at a legible width. This is a TAIL, never a sample: nothing is dropped
 *  from the middle of the series, the order is untouched, and when anything is left off the panel says how
 *  many of how many it is showing. Gaps inside the window keep their slots exactly as before. */
export function candleWindow(candles?:Candle[]|null,width?:number,minStep=CANDLE_MIN_STEP):CandleWindow{
 const all=(candles||[]) as Candle[];
 const w=num(width)||0,step=num(minStep)||CANDLE_MIN_STEP;
 if(!all.length||w<=0||step<=0)return {candles:all,shown:all.length,total:all.length,windowed:false};
 const fits=Math.max(1,Math.floor(w/step));
 if(all.length<=fits)return {candles:all,shown:all.length,total:all.length,windowed:false};
 return {candles:all.slice(all.length-fits),shown:fits,total:all.length,windowed:true};
}
/** The one sentence that keeps a windowed chart honest, and '' when the whole series is on screen. It is one
 *  of the lines the block's "How to read this" panel carries; the short form below stands under the chart. */
export function candleWindowText(window?:CandleWindow|null){
 if(!window?.windowed)return '';
 return `Showing the latest ${window.shown} of ${window.total} candles in this series; the panel is too narrow for all of them.`;
}
/** "Latest 88 of 364 candles" — the same fact, as one short line under the chart instead of a paragraph over it. */
export function candleWindowShort(window?:CandleWindow|null){
 if(!window?.windowed)return '';
 return `Latest ${window.shown} of ${window.total} candles`;
}
/** "26 of 364 stored candles drawn" — what stands under the chart when the whole series fits on screen. */
export function candleDrawnText(drawn?:unknown,bars?:unknown){
 const d=num(drawn),b=num(bars);
 if(d==null||d<=0)return '';
 const n=Math.round(d),total=b==null?null:Math.round(b);
 return total!=null&&total>n?`${n} of ${total} stored candles drawn`:`${n} stored candle${n===1?'':'s'} drawn`;
}
/** Three rupee labels down the candle box: the high, the midpoint and the low, dropped where they collide. */
export function candleAxis(scaled?:ScaledCandles|null,height?:number){
 const h=num(height)||0;
 if(!scaled||h<=0)return [] as {v:number;y:number}[];
 const span=(scaled.hi-scaled.lo)||1;
 const out:{v:number;y:number}[]=[];
 for(const v of [scaled.hi,(scaled.hi+scaled.lo)/2,scaled.lo]){
  const y=h-((v-scaled.lo)/span)*h;
  if(!out.some(t=>Math.abs(t.y-y)<10))out.push({v,y});
 }
 return out;
}
/** A tight rupee label for a chart axis: "25,120" / "1,412.5" / "84.25". Not captured is a dash. */
export function priceTick(v:unknown){
 const n=num(v);if(n==null)return DASH;
 const places=Math.abs(n)>=1000?0:Math.abs(n)>=100?1:2;
 return n.toLocaleString('en-IN',{minimumFractionDigits:places,maximumFractionDigits:places});
}
/** The clock for a 15-min candle, the date for a daily one. Unreadable stamps are a dash. */
export function candleAxisLabel(at?:string|null,intervalKey:string=FUTURES_DEFAULT_INTERVAL){
 if(intervalKey==='1d')return shortDate(at)||DASH;
 return clock(at);
}
/** The sessions a stored series covers, oldest first: the index and the ISO date of the FIRST candle of each.
 *  Read off the stamps that were stored, so a session with no candles simply is not one. */
export function candleSessions(candles?:Candle[]|null){
 const out:{i:number;iso:string}[]=[];
 let last='';
 (candles||[]).forEach((c,i)=>{
  const iso=stamp(c?.at)?.iso;
  if(!iso||iso===last)return;
  last=iso;out.push({i,iso});
 });
 return out;
}
/** A few x labels taken from candles that were actually stored, evenly spaced across the ones there are.
 *
 *  A 15-minute series is labelled by the CLOCK only while it is one session. The moment it crosses a day the
 *  clock is the wrong label: 15:30 comes round again every session, so evenly spaced clock labels across
 *  four days read "13:15 … 11:15 … 15:30" — three times that are not in order and do not describe the span
 *  at all. A series that crosses days is therefore labelled by the DAY, at the first candle of each, which
 *  is the only label on a multi-session axis that is both distinct and in order. Nothing about the candles
 *  changes; this is which of their own stamps is printed under them. */
export function candleAxisTimes(candles?:Candle[]|null,intervalKey:string=FUTURES_DEFAULT_INTERVAL,want=4,
 plotWidth?:number){
 const list=(candles||[]) as Candle[];
 const sessions=candleSessions(list);
 const byDay=intervalKey!=='1d'&&sessions.length>1;
 const marks=byDay
  ?sessions.map(s=>({i:s.i,label:shortDate(list[s.i]?.at)})).filter(m=>!!m.label&&m.label!==DASH)
  :list.map((c,i)=>({i,label:candleAxisLabel(c?.at,intervalKey)})).filter(m=>m.label!==DASH&&m.label!=='');
 let picked=marks;
 if(marks.length>want){
  const step=(marks.length-1)/(want-1);
  picked=[];
  for(let k=0;k<want;k++){
   const mark=marks[Math.round(k*step)];
   if(mark&&!picked.some(m=>m.i===mark.i))picked.push(mark);
  }
 }
 return thinAxisLabels(picked,list.length,plotWidth);
}
/** How much room one x label needs before the next one may be drawn beside it. */
export const CANDLE_LABEL_PX=46;
/** Sessions are not the same length — a window can open three candles before a day ends — so two day labels
 *  can land on the same few pixels. A label that would be drawn on top of the one before it is DROPPED, never
 *  stacked, exactly as the rupee axis already drops a tick that would collide. Without a measured width
 *  nothing is dropped: there is no claim to make about pixels we have not been given. */
export function thinAxisLabels<T extends {i:number}>(picked:T[],count:number,plotWidth?:number,
 pitch=CANDLE_LABEL_PX){
 const width=num(plotWidth);
 if(width==null||width<=0||!count)return picked;
 const step=width/count;
 const out:T[]=[];
 for(const mark of picked){
  const prev=out[out.length-1];
  if(!prev||(mark.i-prev.i)*step>=pitch)out.push(mark);
 }
 return out;
}
/** What a screen reader hears about the panel: the contract, the interval, how many candles and over what
 *  window. It describes the stored series and stops there. */
export function futuresChartSpoken(body?:{contract?:FuturesContractRef|null;candles?:Candle[]|null}|null,
 intervalKey:string=FUTURES_DEFAULT_INTERVAL,symbol?:string|null){
 const candles=body?.candles||[];
 const drawn=scaleCandles(candles,100,100)?.drawn||0;
 const choice=FUTURES_INTERVALS.find(c=>c.key===intervalKey);
 const what=`${futuresChartName(body?.contract,symbol)}, ${(choice?.label||'candles').toLowerCase()}.`;
 if(!drawn)return `${what} ${FUTURES_NO_CANDLES}.`;
 // the same rule as the axis: a range read out as two clock times is only true inside one session
 const multi=intervalKey!=='1d'&&candleSessions(candles).length>1;
 const edge=(c?:Candle|null)=>{
  const s=stamp(c?.at);
  if(!s)return DASH;
  if(intervalKey==='1d')return shortDate(c?.at)||DASH;
  return multi?(s.time?`${shortDate(c?.at)} ${s.time}`:shortDate(c?.at)):(s.time||DASH);
 };
 const first=edge(candles[0]),last=edge(candles[candles.length-1]);
 const window=first!==DASH&&last!==DASH?` from ${first} to ${last}`:'';
 return `${what} ${drawn} candle${drawn===1?'':'s'} stored${window}.`;
}

// --- one symbol for the whole ΔOI block -----------------------------------------------------------------------
/** The block resolves its symbol ONCE and every panel in it is pointed at that one answer — the futures chart
 *  and all ten ΔOI tiles, never one of them resolving its own. A symbol chosen in Customize, or by a click on
 *  a screener row, always wins. With nothing chosen the block falls back to the underlying with the most
 *  premium traded at this 15-min reading, and failing that to the first index (the premium list is empty
 *  whenever the newest reading was rebuilt from candles, which carry no VWAP). A fallback is FLAGGED, so it is
 *  never mistaken for the reader's own choice. */
export type BlockSymbol={symbol:string;defaulted:boolean;label:string};
export const BLOCK_DEFAULT_PREMIUM='Busiest by premium',BLOCK_DEFAULT_INDEX='Default';
export function resolveBlockSymbol(chosen?:string|null,byPremium?:string|null,byIndex?:string|null):BlockSymbol{
 const picked=String(chosen||'').trim();
 if(picked)return {symbol:picked,defaulted:false,label:''};
 const premium=String(byPremium||'').trim();
 if(premium)return {symbol:premium,defaulted:true,label:BLOCK_DEFAULT_PREMIUM};
 const index=String(byIndex||'').trim();
 if(index)return {symbol:index,defaulted:true,label:BLOCK_DEFAULT_INDEX};
 return {symbol:'',defaulted:false,label:''};
}

// =================================================================================================================
// THE FIVE SESSION BLOCKS
//
// Screener · PCR · max pain · IV · futures build-up. Everything below is pure: it formats what the server sent and
// it decides nothing the server did not say. Three rules run through all of it.
//
//  1. A FILTER IS ACTIVE ONLY WHEN THE SERVER SAYS IT APPLIED IT. Not when the browser sent it, not when it is in
//     the reader's rule list, not when the parameter looks right. `applied` is the statement and nothing else is.
//     This cost a day: a sample-size filter rendered as active while it silently deleted every row.
//  2. A NUMBER THAT IS NOT THERE KEEPS ITS REASON. A null with a reason is that reason in words, never a blank and
//     never a drawn point.
//  3. NOTHING DESCRIBES WHAT COMES NEXT. Every direction below says what a number DID across the readings behind
//     it. There is no lean, no level and no call (§5).
// =================================================================================================================

// --- the screener's query -------------------------------------------------------------------------------------
/** The screener route. It is its own endpoint because seven of its filters are signals the older lists never
 *  took - volume against a contract's own median, volume to OI, the two OI changes, the build-up kind, the
 *  moneyness bucket and index-against-stock. */
export const SCREENER_PATH='/api/derivatives/screener';
/** One rule turned into the parameter the screener route takes, or null when the rule carries nothing to send.
 *  The translations are the SAME inclusive ones the tab already uses for the shared columns, so a rule means the
 *  same thing whichever list answers it. */
export function screenerParam(rule:FilterRule):{key:string;value:string}|null{
 const column=filterColumn(rule?.column);
 if(!column)return null;
 // A parameter this route has none of is a 400 that NAMES it, not a silent drop, so it is never sent at all.
 if(column.noScreener)return null;
 const value=String(rule?.value||'').trim();
 if(!value)return null;
 switch(rule.column){
  case 'underlying':return {key:'underlying',value};
  case 'expiry':return {key:'expiry',value};
  // "is not CE" is the other type outright: the server serves exactly two option types, so the complement is exact.
  case 'optionType':return {key:'option_type',value:rule.operator==='is_not'?(value==='CE'?'PE':'CE'):value};
  // "less than N days" is the server's inclusive max_dte = N-1; days-to-expiry is served as a whole number.
  case 'dte':return {key:'max_dte',value:String(Math.max(0,Math.round(Number(value))-1))};
  // "greater than V" is the server's inclusive min_premium_cr = V + 0.01; premium is served rounded to paise.
  case 'premium':return {key:'min_premium_cr',value:String(Math.round((Number(value)+0.01)*100)/100)};
  // "all underlyings" is not a narrowing, so nothing is sent and the server's own default stands.
  case 'watchlist':return value==='all'?null:{key:'watchlist',value};
  case 'market':return {key:'underlying_kind',value};
  case 'volumeRatio':return {key:'min_volume_ratio',value};
  case 'volumeToOi':return {key:'min_volume_to_oi',value};
  // the two OI changes read either way round, so the direction picks which bound is sent
  case 'oiChange15m':return {key:rule.operator==='lt'?'max_oi_change_15m_pct':'min_oi_change_15m_pct',value};
  case 'oiChangeDay':return {key:rule.operator==='lt'?'max_oi_change_day_pct':'min_oi_change_day_pct',value};
  case 'buildup':return {key:'buildup',value};
  case 'moneyness':return {key:'moneyness',value};
  default:return null;
 }
}
/** The query string for the screener. Only rules that survived sanitising are sent, so a junk rule restored from
 *  localStorage never reaches the wire.
 *
 *  Two extras ride alongside the rules. `buildup_window` says which of the two build-up labels the `buildup` rule
 *  reads, and is sent ONLY with that rule — §3.1's two windows are never mixed in one answer. `at` pins the
 *  15-minute reading: omitted, the server reads its newest, and a reader who steps back to an earlier one is
 *  choosing it explicitly rather than being quietly moved there. */
export function screenerQuery(rules:FilterRule[],options?:{buildupWindow?:string;at?:string}){
 const parts:string[]=[];
 let wantsBuildup=false;
 for(const rule of sanitizeRules(rules)){
  const param=screenerParam(rule);
  if(!param)continue;
  if(param.key==='buildup')wantsBuildup=true;
  parts.push(`${param.key}=${encodeURIComponent(param.value)}`);
 }
 const window=String(options?.buildupWindow||'').trim();
 if(wantsBuildup&&BUILDUP_WINDOWS.some(w=>w.value===window))parts.push(`buildup_window=${window}`);
 const at=String(options?.at||'').trim();
 if(at)parts.push(`at=${encodeURIComponent(at)}`);
 return parts.length?`?${parts.join('&')}`:'';
}

// --- the filter-applied invariant ---------------------------------------------------------------------------
// This is the guard the owner paid a day for. Four states, and only ONE of them renders as an active filter.
/** `applied` - the server said it applied this filter, so the rows on screen are narrowed by it.
 *  `pending` - the server has not answered yet, or answered without saying. Nothing is claimed either way.
 *  `not_applied` - the server knows this filter and did NOT apply it. The rows are not narrowed by it.
 *  `unsupported` - the server does not know this parameter at all. */
export type FilterState='applied'|'pending'|'not_applied'|'unsupported';
export type FilterStatus={rule:FilterRule;key:string;label:string;text:string;state:FilterState;reason:string};
export const FILTER_PENDING_REASON='The server has not said whether it applied this filter, so it is not shown as active.';
export const FILTER_NOT_APPLIED_REASON='The server did not apply this filter, so the rows below are not narrowed by it.';
export const FILTER_UNSUPPORTED_REASON='This server has no parameter for this filter, so it was not applied.';
/** "a, b and c" - one list in plain words, so a sentence about three filters reads as a sentence. */
export function joinWords(items:string[]){
 const list=(items||[]).filter(Boolean);
 if(!list.length)return '';
 if(list.length===1)return list[0];
 return `${list.slice(0,-1).join(', ')} and ${list[list.length-1]}`;
}
/** The one place a filter's state is decided. `body` is the screener's response; a null body (loading, error, a
 *  route that is not there yet) leaves EVERY rule pending - never active. An `applied` list the server did not
 *  send is not an empty list, it is silence, and silence is pending. */
/** Pulls a key out of an entry of `applied` / `available_filters`. Both are lists of OBJECTS carrying `key`; a
 *  bare string is accepted too so a leaner server never reads as silence. */
export function filterKey(entry:unknown){
 if(typeof entry==='string')return entry;
 const row=(entry||{}) as Record<string,unknown>;
 return typeof row.key==='string'?row.key:'';
}
/** The filter list the SERVER offers. It is `available_filters`, or `filters.available` — NEVER `available`, which
 *  on this route, as on all eight others, is the envelope's boolean for "is the F&O store readable". Reading a
 *  boolean as a list would silently make every filter unsupported. */
export function servedFilters(body?:{available_filters?:unknown;filters?:{available?:unknown}|null}|null){
 const list=Array.isArray(body?.available_filters)?body?.available_filters
  :Array.isArray(body?.filters?.available)?body?.filters?.available:null;
 return (list||null) as {key?:string;ready?:boolean;text?:string;missing_columns?:string[]}[]|null;
}
/** The filters the query ACTUALLY applied, likewise from `applied` or `filters.applied`. */
export function servedApplied(body?:{applied?:unknown;filters?:{applied?:unknown}|null}|null){
 const list=Array.isArray(body?.applied)?body?.applied
  :Array.isArray(body?.filters?.applied)?body?.filters?.applied:null;
 return (list||null) as {key?:string;value?:unknown;always?:boolean;text?:string}[]|null;
}
export function filterStatuses(rules:FilterRule[],
 body?:{applied?:unknown;available_filters?:unknown;filters?:{applied?:unknown;available?:unknown}|null}|null,
 labels?:(rule:FilterRule)=>string):FilterStatus[]{
 const clean=sanitizeRules(rules);
 const appliedList=servedApplied(body);
 const said=Array.isArray(appliedList);
 const applied=new Map<string,{value?:unknown;always?:boolean;text?:string}>();
 for(const row of appliedList||[]){const key=filterKey(row);if(key)applied.set(key,row as any);}
 const offered=servedFilters(body);
 const knows=Array.isArray(offered);
 const available=new Map<string,{ready?:boolean;text?:string;missing_columns?:string[]}>();
 for(const row of offered||[]){const key=filterKey(row);if(key)available.set(key,row as any);}
 return clean.map(rule=>{
  const param=screenerParam(rule);
  const column=filterColumn(rule.column);
  const key=param?param.key:(column?.param||String(rule.column));
  const offer=available.get(key);
  const label=columnLabel(rule.column);
  const text=ruleText(rule,labels?.(rule));
  let state:FilterState='pending';
  if(!said)state='pending';
  else if(applied.has(key))state='applied';
  // a rule this route has no parameter for was never sent, and a rule the store cannot answer is not one either
  else if(column?.noScreener)state='unsupported';
  else if(knows&&!available.has(key))state='unsupported';
  else if(offer&&offer.ready===false)state='unsupported';
  else state='not_applied';
  // The server's own words always win over ours. For an unsupported filter that is the columns it is missing;
  // ours is only there so a state is never left unexplained.
  const missing=(offer?.missing_columns||[]).filter(Boolean);
  const reason=state==='applied'?''
   :state==='pending'?FILTER_PENDING_REASON
   :state==='unsupported'?(column?.noScreener
     ?'The screener has no parameter for this filter, so it was not sent. It still narrows the other lists on this tab.'
     :missing.length?`This store does not carry ${joinWords(missing)}, so this filter could not be applied.`
     :FILTER_UNSUPPORTED_REASON)
   :FILTER_NOT_APPLIED_REASON;
  return {rule,key,label,text,state,reason};
 });
}
/** The §3 floors and the reading the screener applies to EVERY query, in the server's own words. They are not the
 *  reader's filters and never appear in the reader's strip, but they are the reason a row is on screen, so they
 *  belong in what the block explains. */
export function alwaysApplied(body?:{applied?:unknown;filters?:{applied?:unknown}|null}|null){
 return (servedApplied(body)||[]).filter(row=>row&&row.always)
  .map(row=>String(row.text||'').trim()).filter(Boolean);
}
/** How many filters are genuinely narrowing the rows on screen. This is the ONLY number the header may call a
 *  filter count for the screener: a rule the server did not apply is not a filter in force. */
export function appliedCount(statuses:FilterStatus[]){return (statuses||[]).filter(s=>s.state==='applied').length;}
/** The sentence that goes where the reader is looking when a filter did not run. It names the filters, plainly,
 *  and says the rows are not narrowed by them. Empty when every filter applied. */
export function notAppliedText(statuses:FilterStatus[],served?:string|null){
 const list=(statuses||[]).filter(s=>s.state!=='applied');
 if(!list.length)return '';
 if(served&&String(served).trim())return String(served).trim();
 const name=(s:FilterStatus)=>s.label.toLowerCase();
 const off=list.filter(s=>s.state==='not_applied'),out=list.filter(s=>s.state==='unsupported'),
  wait=list.filter(s=>s.state==='pending');
 const said:string[]=[];
 if(off.length)said.push(`The server did not apply ${joinWords(off.map(name))}.`);
 if(out.length)said.push(`The server has no parameter for ${joinWords(out.map(name))}.`);
 if(wait.length)said.push(`The server has not said whether it applied ${joinWords(wait.map(name))}.`);
 return `${said.join(' ')} The rows below are NOT narrowed by ${list.length===1?'it':'them'}.`;
}
/** The screener's subtitle: what is actually in force, never what was asked for. */
export function appliedText(statuses:FilterStatus[]){
 const on=(statuses||[]).filter(s=>s.state==='applied');
 if(on.length)return on.map(s=>s.text).join(' · ');
 return (statuses||[]).length?'No filter applied by the server':'No filters — every contract over the liquidity floors';
}

// --- one symbol for the WHOLE tab -------------------------------------------------------------------------------
/** The tab resolves its symbol ONCE and every block is pointed at that one answer: the chain, the strikes, the ΔOI
 *  tiles, the futures chart, PCR, max pain, IV and the futures build-up. A symbol chosen in Customize wins; failing
 *  that the last row the reader clicked anywhere on the tab; failing that the defaults `resolveBlockSymbol` already
 *  applies, still flagged as defaults. No block resolves its own - that is how two panels end up describing two
 *  different underlyings under one as-of line. */
export function resolveTabSymbol(chosen?:string|null,clicked?:string|null,byPremium?:string|null,
 byIndex?:string|null):BlockSymbol{
 const picked=String(chosen||'').trim()||String(clicked||'').trim();
 return resolveBlockSymbol(picked,byPremium,byIndex);
}
/** What the badge says. A defaulted symbol always says it is a default; a chosen one is just itself. */
export function symbolBadge(choice?:BlockSymbol|null){
 if(!choice?.symbol)return '';
 return choice.defaulted&&choice.label?`${choice.label}: ${choice.symbol}`:choice.symbol;
}
/** The one sentence a block prints when the tab has no symbol at all. */
export const NO_SYMBOL_TEXT='Choose a symbol in Customize, or click any row on this tab, to point every block at it.';

// --- directions: what a number DID across the readings behind it -------------------------------------------------
// Four vocabularies, all the owner's own words, all past tense. None of them is a call.
/** PCR: what the ratio did. */
export const PCR_CHIPS:Record<string,string>={rising:'↑ RISING',flat:'→ STABLE',falling:'↓ FALLING'};
/** Max pain, in the owner's exact words: Shifting Up · Stable · Shifting Down. */
export const MAX_PAIN_CHIPS:Record<string,string>={shifting_up:'↑ SHIFTING UP',stable:'→ STABLE',
 shifting_down:'↓ SHIFTING DOWN'};
/** IV, in the owner's exact words: Expanding · Stable · Cooling. */
export const IV_CHIPS:Record<string,string>={expanding:'↑ EXPANDING',stable:'→ STABLE',cooling:'↓ COOLING'};
/** Futures OI, the same three words the ΔOI tiles already use. */
export const FUTURES_CHIPS:Record<string,string>={building:'↑ BUILDING',flat:'→ FLAT',unwinding:'↓ UNWINDING'};
/** A direction the server did not send, or could not compute, gets NO chip - it is a state, not a fourth word. */
export const NO_SESSION_DIRECTION='no baseline';
/** The chip for one of the four vocabularies. An unknown key gets no chip at all rather than a guessed one. */
export function sessionChip(chips:Record<string,string>,direction:unknown){
 return chips[String(direction||'')]||'';
}
/** Green for the first word, red for the third, neutral for the middle. Colour repeats the word, never adds to it. */
export function sessionTone(chips:Record<string,string>,direction:unknown):'up'|'down'|'flat'{
 const keys=Object.keys(chips),key=String(direction||'');
 if(key===keys[0])return 'up';
 if(key===keys[2])return 'down';
 return 'flat';
}
/** How many readings back a session direction is read over: four, one hour, the window the ΔOI tiles already use. */
export const SESSION_LOOKBACK=4;
/** Inside this fraction of the series' own span the number is called stable rather than moved. */
export const SESSION_FLAT_FRACTION=0.05;
export const PCR_KEYS=['rising','flat','falling'],MAX_PAIN_KEYS=['shifting_up','stable','shifting_down'];
export const IV_KEYS=['expanding','stable','cooling'],FUTURES_KEYS=['building','flat','unwinding'];
/** Reads a direction off the very values a panel drew, so a chip can never disagree with its own chart. The
 *  server computes the same reading and serves it; neither is trusted over the other, because both are the same
 *  rule over the same points. Under two captured values there is no baseline and no chip. */
export function sessionDirection(values:(number|null|undefined)[]|null|undefined,keys:string[],
 lookback=SESSION_LOOKBACK,flat=SESSION_FLAT_FRACTION){
 const list=(values||[]).map(v=>num(v));
 const real=list.map((v,i)=>({v,i})).filter((p):p is {v:number;i:number}=>p.v!=null);
 if(real.length<2)return NO_SESSION_DIRECTION;
 const last=real[real.length-1];
 // the reading `lookback` captured values back, or the oldest there is when the session is younger than that
 const earlier=real[Math.max(0,real.length-1-lookback)];
 const change=last.v-earlier.v;
 const span=Math.max(...real.map(p=>Math.abs(p.v-earlier.v)),Math.abs(change))||Math.abs(last.v)||1;
 if(Math.abs(change)<=span*flat)return keys[1];
 return change>0?keys[0]:keys[2];
}
/** The direction the SERVER sent when it sent one, and the one read off the drawn points when it did not. The
 *  server's word wins, because it saw every reading and a panel may be drawing a window of them. */
export function servedDirection(served:unknown,values:(number|null|undefined)[]|null|undefined,keys:string[]){
 const key=String(served||'').trim();
 if(keys.includes(key))return key;
 return sessionDirection(values,keys);
}

// --- PCR through the session --------------------------------------------------------------------------------
export const PCR_OI_LABEL='PCR by open interest',PCR_VOLUME_LABEL='PCR by volume';
export const PCR_DEFINITION='PCR by open interest is put open interest divided by call open interest across every strike of this expiry, at each 15-min reading. PCR by volume is the same division over the day\'s volume (§3.5).';
export const PCR_READING_TEXT='Both lines are the ratios as captured. The chip says what the open-interest ratio did over the last hour of readings, and nothing about what it does after that.';
export const PCR_NO_POINTS='No put-call ratio has been captured for this symbol yet';
export const PCR_THIN_CHAIN='Withheld: the chain was too thin at this reading to divide honestly.';
// --- max pain through the session ----------------------------------------------------------------------------
export const MAX_PAIN_DEFINITION='Max pain is the strike where the total payout to option buyers at expiry would be smallest, worked out from the open interest standing at that 15-min reading (§3.6).';
export const MAX_PAIN_GAP_TEXT='The second line is the captured spot at the same readings, on the same scale, so the gap between the two is the gap you can see.';
export const MAX_PAIN_READING_TEXT='The chip says which way the max-pain strike moved over the last hour of readings. It describes the standing book, and says nothing about where either number goes.';
export const MAX_PAIN_NO_POINTS='No max-pain strike has been computed for this symbol yet';
export const MAX_PAIN_THIN_CHAIN='Withheld: too few strikes cleared the liquidity floors to compute max pain at this reading.';
// --- IV through the session ------------------------------------------------------------------------------------
// The ONE number on this tab the exchange never said. Every other figure here is something a venue reported; this
// one is what a model returned when it was asked which volatility reproduces a traded price. It is labelled as
// computed wherever it appears - beside the block title, on the chart, over the strike list and in what a screen
// reader hears - not once in a footnote at the bottom.
/** The short label that rides beside every implied volatility on screen. */
export const IV_COMPUTED_TAG='COMPUTED';
/** The long form, for a screen reader and for the block's definitions. */
export const IV_COMPUTED_TEXT='Implied volatility is COMPUTED here, not reported by the exchange: it is what a pricing model returns when it is asked which volatility reproduces the traded price. Every other number on this tab is something the exchange said.';
export const IV_ATM_LABEL='ATM implied volatility';
export const IV_DEFINITION='The line is the at-the-money implied volatility of this underlying at each 15-min reading; the list beside it is the latest solved volatility per strike.';
export const IV_READING_TEXT='The chip says what the at-the-money volatility did over the last hour of readings. It describes what the model solved from captured prices, and nothing after them.';
export const IV_NO_POINTS='No implied volatility has been computed for this symbol yet';
export const IV_THIN_CHAIN='Withheld: the chain was too thin at this reading for an at-the-money volatility.';
/** Why a solver returned nothing. The server's own `reason_text` always wins; these are here so a null is never
 *  left unexplained when it arrives with a bare reason code. */
export const IV_REASONS:Record<string,string>={
 stale_trade:'No volatility: the last trade in this contract is older than the reading it would be solved at.',
 no_time_value:'No volatility: at this price the contract has no time value left to solve.',
 below_intrinsic:'No volatility: the traded price is under the contract\'s intrinsic value, so no volatility reproduces it.',
 no_convergence:'No volatility: the solver did not settle on an answer for this price.',
 expiry_today:'No volatility: this contract expires today, so there is no time left to price.',
};
/** The words for one null. An unknown reason code is said as itself rather than swallowed - a reason we cannot
 *  translate is still a reason, and the reader is owed it. */
export function ivReasonText(reason?:string|null,served?:string|null){
 const text=String(served||'').trim();
 if(text)return text;
 const key=String(reason||'').trim();
 if(!key)return '';
 return IV_REASONS[key]||`No volatility: the server gave the reason "${key}".`;
}
/** An implied volatility in words. Served as a fraction (0.184) or as a percentage (18.4); both read as "18.4%".
 *  A null is the reason it is null, never a blank and never a zero. */
export function ivText(v:unknown){
 const n=num(v);
 if(n==null)return DASH;
 const pct=Math.abs(n)<=3?n*100:n;
 return `${pct.toFixed(1)}%`;
}
/** The same number with the computed label welded on, for anywhere it appears away from the block's own label. */
export function ivTagged(v:unknown){const text=ivText(v);return text===DASH?text:`${text} ${IV_COMPUTED_TAG}`;}
/** The model and the rate it solved at, in one line. A model the server did not name is said to be unnamed
 *  rather than guessed at. */
export function ivModelText(body?:{model?:string|null;rate?:unknown;rate_label?:string|null}|null){
 const model=String(body?.model||'').trim();
 const rate=num(body?.rate);
 const rateText=String(body?.rate_label||'').trim()
  ||(rate==null?'':`${(Math.abs(rate)<=1?rate*100:rate).toFixed(2)}% risk-free rate`);
 if(!model&&!rateText)return 'The server did not name the model or the rate these were solved at.';
 return `Model: ${model||'not named by the server'}${rateText?` · solved at ${rateText}`:''}.`;
}
/** The block's one computed sentence: the server's when it sends one, ours when it does not. */
export function ivComputedText(body?:{computed_text?:string|null}|null){
 return String(body?.computed_text||'').trim()||IV_COMPUTED_TEXT;
}
/** How many of the readings carried a volatility, and how many were nulls with a reason. Both counted, because a
 *  line drawn through four points out of twenty is not the same picture as one drawn through twenty. */
export function ivCoverage(points?:{iv?:unknown;reason?:string|null}[]|null){
 const list=points||[];
 let solved=0,withReason=0;
 for(const p of list){if(num(p?.iv)!=null)solved++;else if(String(p?.reason||'').trim())withReason++;}
 return {total:list.length,solved,withReason};
}
/** "14 of 20 readings solved; 6 carry a reason." Empty when there is nothing captured to count. */
export function ivCoverageText(points?:{iv?:unknown;reason?:string|null}[]|null){
 const {total,solved,withReason}=ivCoverage(points);
 if(!total)return '';
 return `${solved} of ${total} reading${total===1?'':'s'} solved`
  +(withReason?`; ${withReason} carr${withReason===1?'ies':'y'} a reason instead of a number.`:'.');
}
/** What a screen reader hears about one strike's volatility - the number and that it was computed, or the reason
 *  there is none. */
export function ivStrikeSpoken(row?:{strike?:unknown;option_type?:unknown;moneyness?:string|null;iv?:unknown;
 reason?:string|null;reason_text?:string|null}|null){
 const what=`${strike(row?.strike)} ${String(row?.option_type||'').toUpperCase()}`.trim();
 const where=String(row?.moneyness||'').trim();
 const value=num(row?.iv)!=null?`computed implied volatility ${ivText(row?.iv)}`
  :(ivReasonText(row?.reason,row?.reason_text)||'no computed implied volatility');
 return `${what}${where?`, ${where}`:''}. ${value}.`;
}

// --- futures build-up -------------------------------------------------------------------------------------------
export const FUTURES_BUILDUP_DEFINITION='Open interest on the front futures contract at each 15-min reading, against its own 20-day average, with the basis - the futures price less spot - beside it (§3.7).';
export const FUTURES_BUILDUP_READING_TEXT='The chip says what open interest on this contract did over the last hour of readings. It describes the standing position and stops there.';
export const FUTURES_BUILDUP_NO_POINTS='No futures readings have been captured for this contract yet';
export const FUTURES_BUILDUP_THIN='Withheld: this contract did not clear the liquidity floors at this reading.';
/** "+₹30.00 (+0.12%)" - the basis and its share of spot together, because one without the other says little.
 *  Either half missing keeps the other; both missing is a dash. */
export function basisPair(basis:unknown,pct:unknown){
 const b=num(basis),p=num(pct);
 if(b==null&&p==null)return DASH;
 if(b==null)return signed(p,2);
 if(p==null)return basisText(b);
 return `${basisText(b)} (${signed(p,2)})`;
}
/** "1.24× its 20-day average, from 20 sessions" - the share and what it was built from. Fewer sessions than the
 *  average claims is said outright rather than rounded over. */
export function oiVsAvgText(share:unknown,sessions?:unknown){
 const n=num(share);
 if(n==null)return NO_BASELINE;
 const s=num(sessions);
 const whole=s==null?null:Math.round(s);
 return `${n.toFixed(2)}${TIMES} its 20-day average`
  +(whole==null?'':`, from ${whole} session${whole===1?'':'s'}`);
}
// --- one line chart, one set of axis rules ----------------------------------------------------------------------
// Every one of the four session blocks draws the same picture: one or two series of 15-min readings across one
// session. They share this scaling and these axes so a rule fixed once is fixed everywhere - and because the axis
// bug that shipped (labels running backwards) is exactly the kind a second copy reintroduces.
/** Maps a list of values onto a box. A reading with no value stays null, so a gap is drawn as a gap. Two or more
 *  real values are needed before there is a line at all. */
export function scaleValues(values:(number|null|undefined)[]|null|undefined,width:number,height:number,
 includeZero=false):Scaled|null{
 const list=(values||[]).map(v=>num(v));
 const real=list.filter((v):v is number=>v!=null);
 if(real.length<2||width<=0||height<=0)return null;
 const lo=includeZero?Math.min(0,...real):Math.min(...real);
 const hi=includeZero?Math.max(0,...real):Math.max(...real);
 const span=(hi-lo)||Math.abs(hi)||1;
 const step=list.length>1?width/(list.length-1):0;
 return {lo,hi,points:list.map((v,i)=>v==null?null:{x:i*step,y:height-((v-lo)/span)*height,i})};
}
/** Maps TWO series onto ONE box and one scale, for the only case where that is honest: two numbers in the same
 *  unit that are read against each other (max pain against spot - both rupee strikes). Anything measured in a
 *  different unit gets its own scale, exactly as the ΔOI tiles do. */
export function scaleTogether(a:(number|null|undefined)[]|null|undefined,b:(number|null|undefined)[]|null|undefined,
 width:number,height:number):{lo:number;hi:number;a:Scaled|null;b:Scaled|null}|null{
 const first=(a||[]).map(v=>num(v)),second=(b||[]).map(v=>num(v));
 const real=[...first,...second].filter((v):v is number=>v!=null);
 if(real.length<2||width<=0||height<=0)return null;
 const lo=Math.min(...real),hi=Math.max(...real),span=(hi-lo)||Math.abs(hi)||1;
 const map=(list:(number|null)[]):Scaled|null=>{
  if(!list.some(v=>v!=null))return null;
  const step=list.length>1?width/(list.length-1):0;
  return {lo,hi,points:list.map((v,i)=>v==null?null:{x:i*step,y:height-((v-lo)/span)*height,i})};
 };
 return {lo,hi,a:map(first),b:map(second)};
}
/** How much vertical room one y label needs before the next may be drawn under it. */
export const VALUE_LABEL_PX=11;
/** The y labels down a line chart: `want` values evenly spaced from the low to the high, dropped where they would
 *  collide. They are returned HIGH FIRST, which is also top-to-bottom on screen - so the values run strictly
 *  DOWNWARD through the list and strictly UPWARD up the box. An axis that runs the other way is the bug that
 *  shipped once; it is built in one place here so it can be checked in one place. */
export function valueAxis(scaled:Scaled|null,height:number,want=3,pitch=VALUE_LABEL_PX){
 if(!scaled||!(height>0))return [] as {v:number;y:number}[];
 const span=(scaled.hi-scaled.lo)||1;
 const steps=Math.max(2,Math.round(want));
 const out:{v:number;y:number}[]=[];
 for(let k=steps-1;k>=0;k--){
  const v=scaled.lo+span*(k/(steps-1));
  const y=height-((v-scaled.lo)/span)*height;
  // a label that would sit on the one before it is DROPPED, never stacked
  if(out.some(t=>Math.abs(t.y-y)<pitch))continue;
  // and a value equal to one already taken is dropped too: two ticks reading the same thing is not an axis
  if(out.some(t=>t.v===v))continue;
  out.push({v,y});
 }
 return out;
}
/** The same axis, already formatted, with any tick whose LABEL repeats the one before it dropped. Two ticks that
 *  both print "0.88" tell a reader the axis is broken, whatever the underlying values were. */
export function valueAxisLabels(scaled:Scaled|null,height:number,format:(v:number)=>string,want=3){
 const out:{v:number;y:number;label:string}[]=[];
 for(const tick of valueAxis(scaled,height,want)){
  const label=format(tick.v);
  if(!label||label===DASH)continue;
  if(out.some(t=>t.label===label))continue;
  out.push({...tick,label});
 }
 return out;
}
/** The x labels under a line chart: clock times taken from readings that were actually captured, evenly spaced,
 *  each one DISTINCT and each one LATER than the one before it. A stamp that cannot be read is not a label, a
 *  repeated clock is dropped, and a stamp that does not advance on the one before it is dropped - which is what
 *  a series crossing a day does to a clock axis. Nothing about the readings changes; this is only which of their
 *  own stamps is printed under them. */
export function sessionAxisTimes(times:(string|null|undefined)[]|null|undefined,want=4,plotWidth?:number,
 pitch=CANDLE_LABEL_PX){
 const list=times||[];
 const marks:{i:number;label:string}[]=[];
 let last='';
 list.forEach((t,i)=>{
  const s=stamp(t);
  if(!s||!s.time)return;
  // Strictly increasing IN THE LABEL, not in the stamp behind it. A stamp that is genuinely later can still
  // PRINT an earlier clock — 15:30 on Thursday, then 09:30 on Friday — and an axis reading "13:15 15:30 09:30"
  // is the bug that shipped. The label is what the reader has, so the label is what must run forward: a clock
  // that does not advance on the last one printed is dropped, never drawn out of order and never stacked.
  if(last&&s.time<=last)return;
  last=s.time;
  marks.push({i,label:s.time});
 });
 if(!marks.length)return [] as {i:number;label:string}[];
 let picked=marks;
 if(marks.length>want){
  const step=(marks.length-1)/(Math.max(2,want)-1);
  picked=[];
  for(let k=0;k<want;k++){
   const at=marks[Math.round(k*step)];
   if(at&&!picked.some(m=>m.i===at.i))picked.push(at);
  }
 }
 // distinct labels: two readings a day apart can both read "09:30", and only one of them may be printed
 const seen=new Set<string>();
 const unique=picked.filter(m=>{if(seen.has(m.label))return false;seen.add(m.label);return true});
 return thinAxisLabels(unique,list.length,plotWidth,pitch);
}
/** What a screen reader hears about a session line: what it is, how many readings carried a value, and over what
 *  window. It describes the captured series and stops there. */
export function sessionSpoken(what:string,values:(number|null|undefined)[]|null|undefined,
 times:(string|null|undefined)[]|null|undefined){
 const drawn=(values||[]).filter(v=>num(v)!=null).length;
 const list=times||[];
 if(!drawn)return `${what}. Nothing captured yet.`;
 const from=clock(list[0]),to=clock(list[list.length-1]);
 const window=from!==DASH&&to!==DASH?` from ${from} to ${to}`:'';
 return `${what}. ${drawn} captured reading${drawn===1?'':'s'}${window}.`;
}

// =================================================================================================================
// ALIGNED TO THE SERVED RESPONSES
//
// Everything below reads what server/kanida_pilot/derivatives.py actually sends. Three of its choices shape this
// whole section, and each one removes a way for the tab to be wrong:
//   * The DIRECTION WORDS are served. `direction_words` maps up/down/flat/none onto this series' own vocabulary
//     and `direction_labels` gives each key its reader-facing word, so the tab stops hardcoding "Shifting Up".
//   * Max-pain DISTANCE is strike minus spot. Positive means the strike sits above spot, and the wording below
//     names the subject so the sign can never be read the other way round.
//   * The risk-free rate behind every implied volatility is a CODE CONSTANT with no feed behind it. The server
//     says so, and every solved reading carries what the answer would be a percentage point higher.
// =================================================================================================================

/** The vocabulary for ONE series. Most routes serve a single flat map. The futures route serves TWO, named for
 *  the two readings it carries - `oi` (building / unwinding) and `basis` (widening / narrowing) - because a
 *  contract's open interest and its basis do not move in the same words.
 *
 *  Handing the whole object to a chip that wanted one of them is a QUIET failure: `up` comes back undefined, so
 *  the chip keeps its word but loses its arrow and its colour, and still looks like a chip. This resolves the
 *  vocabulary explicitly and returns nothing when it cannot, so the failure has something to be caught by. */
export function directionWords(served:unknown,which?:string):DirectionWords|null{
 const raw=(served||{}) as Record<string,any>;
 const pick=which?raw[which]:raw;
 return pick&&typeof pick==='object'&&typeof pick.up==='string'&&typeof pick.down==='string'
  ?pick as DirectionWords:null;
}
/** True when there IS a direction to draw and no vocabulary that contains it - the state where a chip would
 *  silently lose its arrow and its colour while still reading as a chip. */
export function directionMissing(direction:unknown,words?:DirectionWords|null){
 const key=String(direction||'').trim();
 if(!key||key===NO_SESSION_DIRECTION)return false;
 if(!words)return true;
 return key!==words.up&&key!==words.down&&key!==words.flat;
}
/** The chip for a served direction: the arrow this tab already uses, over the SERVER's own word for it. A series
 *  that sent no direction, or one whose word we were not given, gets no chip at all rather than a guessed one. */
export function servedChip(direction:unknown,words?:DirectionWords|null,labels?:DirectionLabels|null){
 const key=String(direction||'').trim();
 if(!key||key===NO_SESSION_DIRECTION)return '';
 const label=String(labels?.[key]||'').trim()||key.replace(/_/g,' ');
 const arrow=key===words?.up?'↑':key===words?.down?'↓':key===words?.flat?'→':'';
 return `${arrow?`${arrow} `:''}${label.toUpperCase()}`;
}
/** Green for the series' own "up" word, red for its "down", neutral otherwise. Colour repeats the served word and
 *  adds nothing to it. */
export function servedTone(direction:unknown,words?:DirectionWords|null):'up'|'down'|'flat'{
 const key=String(direction||'').trim();
 if(key&&key===words?.up)return 'up';
 if(key&&key===words?.down)return 'down';
 return 'flat';
}
/** The server's own sentence for what a direction is, or ours when it sent none. */
export function directionRule(body?:{direction_text?:string|null}|null){
 return String(body?.direction_text||'').trim();
}

// --- gaps ---------------------------------------------------------------------------------------------------
/** Every series sits on the store's own reading grid, so a reading an underlying has no row for is a SLOT with no
 *  value and `gap:true`. Pulling one value out of a series therefore has to leave the gap null rather than reach
 *  past it — this is the one place that happens, for every block. */
export function seriesValues<P>(points:P[]|null|undefined,pick:(p:P)=>unknown){
 return (points||[]).map(p=>{
  const row=(p||{}) as Record<string,unknown>;
  if(row.gap===true)return null;
  return num(pick(p));
 });
}
/** The stamps of a series, in order, for the time axis. A gap keeps its stamp: the hole holds its place. */
export function seriesTimes<P extends {at?:string|null}>(points:P[]|null|undefined){
 return (points||[]).map(p=>p?.at??null);
}
/** How much of the session actually carried a value, and why the rest did not — counted from the server's own
 *  tally when it sent one, and off the points when it did not. Never presented as a whole session. */
export function readingsText(body?:{total_readings?:unknown;readings_with_value?:unknown}|null){
 const total=num(body?.total_readings),withValue=num(body?.readings_with_value);
 if(total==null||withValue==null)return '';
 return `${Math.round(withValue)} of ${Math.round(total)} reading${Math.round(total)===1?'':'s'} carried a value`;
}
/** The reasons the rest did not, in the server's own sentences, commonest first. This is what stands where a
 *  thinner symbol would otherwise show an unexplained hole. */
export function withheldLines(body?:{withheld_reasons?:Record<string,number>|null;
 reason_text?:Record<string,string>|null}|null){
 const counts=body?.withheld_reasons||{};
 const text=body?.reason_text||{};
 return Object.entries(counts).filter(([,n])=>num(n)!=null&&Number(n)>0)
  .sort((a,b)=>Number(b[1])-Number(a[1]))
  .map(([key,n])=>`${Math.round(Number(n))} reading${Math.round(Number(n))===1?'':'s'}: ${text[key]||key}`);
}
/** The one line a panel prints when part of its session is missing, and '' when none of it is. */
export function withheldText(body?:{total_readings?:unknown;readings_with_value?:unknown;
 withheld_reasons?:Record<string,number>|null;reason_text?:Record<string,string>|null}|null){
 const lines=withheldLines(body);
 if(!lines.length)return '';
 const counted=readingsText(body);
 return `${counted?`${counted}. `:''}${lines.join(' ')}`;
}

// --- max pain, with the store's own sign ------------------------------------------------------------------------
/** The distance, said with its SUBJECT named so the sign cannot be read backwards. The store's convention is
 *  strike minus spot, so a positive number is the strike sitting above spot, and that is what this says. */
export function maxPainDistanceText(distance:unknown){
 const n=num(distance);
 if(n==null)return DASH;
 if(n===0)return 'strike at spot';
 return `strike ${strike(Math.abs(n))} ${n>0?'above':'below'} spot`;
}
/** The server's own sentence for the convention. Printed, never paraphrased. */
export function maxPainDistanceRule(body?:{distance_definition?:string|null}|null){
 return String(body?.distance_definition||'').trim();
}
/** The one line under the max-pain panel. A spot the store did not capture is a dash and says so, rather than
 *  letting a distance be computed against nothing. */
export function maxPainLine(body?:{latest_max_pain_strike?:unknown;latest_spot?:unknown;latest_distance?:unknown;
 latest_total_oi?:unknown}|null){
 if(!body)return '';
 const oi=num(body.latest_total_oi);
 const spot=num(body.latest_spot);
 // A distance is a strike measured against a spot, so with no spot there is no distance to dash out — the
 // sentence says that once instead of printing two dashes in a row and leaving the reader to join them up.
 const against=spot==null
  ?'no spot was captured at this reading, so there is no distance to it'
  :`spot ${price(spot)} · ${maxPainDistanceText(body.latest_distance)}`;
 return `Max pain ${strike(body.latest_max_pain_strike)} · ${against}.`
  +(oi==null?' Total OI behind it: not captured.':` From ${compact(oi)} contracts of open interest.`);
}

// --- PCR, aligned ------------------------------------------------------------------------------------------------
/** The one line under the PCR panel. */
export function pcrLine(body?:{latest_pcr_oi?:unknown;latest_pcr_volume?:unknown}|null){
 if(!body)return '';
 return `${PCR_OI_LABEL} ${pcrText(body.latest_pcr_oi)} · ${PCR_VOLUME_LABEL} ${pcrText(body.latest_pcr_volume)}.`;
}

// --- implied volatility: what the number rests on -----------------------------------------------------------------
// The COMPUTED label says a model produced it. That is necessary and it is not sufficient: the reader also has to
// be able to find out WHAT it was produced from. The weakest of those inputs is the risk-free rate, which is a
// constant in the server's code with no feed behind it — so it is named on screen, with its own weight beside it,
// rather than left in a footnote.
/** "6.50% a year" — the rate itself, from `risk_free_rate`. */
export function ivRateText(body?:{risk_free_rate?:unknown}|null){
 const rate=num(body?.risk_free_rate);
 if(rate==null)return '';
 return `${(Math.abs(rate)<=1?rate*100:rate).toFixed(2)}% a year`;
}
/** The rate AND where it came from, in one line the reader meets beside the number — not in a footnote. The
 *  server's `risk_free_rate_source.text` is the full sentence; this is the short form that stands on screen. */
export function ivRateSourceShort(body?:{risk_free_rate?:unknown;
 risk_free_rate_source?:{kind?:string;live_feed?:boolean}|null}|null){
 const rate=ivRateText(body);
 const source=body?.risk_free_rate_source;
 if(!rate&&!source)return '';
 const kind=String(source?.kind||'').trim();
 // `live_feed:false` is the fact that matters, so it is said outright rather than implied by the word "constant"
 const feed=source?.live_feed===false?'no live feed behind it'
  :source?.live_feed===true?'from a live feed':'';
 const how=[kind,feed].filter(Boolean).join(', ');
 return `Solved at a risk-free rate of ${rate||'a rate the server did not name'}${how?` — ${how}`:''}.`;
}
/** The server's full sentence about the rate. Shown in the block's one disclosure, word for word. */
export function ivRateSourceText(body?:{risk_free_rate_source?:{text?:string|null;where?:string|null}|null}|null){
 const source=body?.risk_free_rate_source;
 const text=String(source?.text||'').trim();
 const where=String(source?.where||'').trim();
 if(!text)return '';
 return where?`${text} It lives in ${where}.`:text;
}
/** True when the rate is a constant with nothing feeding it — the one caveat on this block worth amber. */
export function ivRateIsAssumed(body?:{risk_free_rate_source?:{live_feed?:boolean}|null}|null){
 return body?.risk_free_rate_source?.live_feed===false;
}
/** "±0.12 points if the rate were 1 point higher" — the weight of that assumption, from the reading itself.
 *  This is the figure that turns "trust the rate" into "here is what it is worth". */
export function ivRateSensitivityText(shift:unknown){
 const n=num(shift);
 if(n==null)return '';
 const size=Math.abs(n);
 if(size<0.005)return 'A rate one percentage point higher would not move this reading at all.';
 return `A rate one percentage point higher would move this reading by ${size.toFixed(2)} percentage `
  +`point${size===1?'':'s'}${n<0?' down':' up'}.`;
}
/** The largest rate shift across the readings drawn, so the block can state the assumption's weight once rather
 *  than per point. Reads the legs, because that is where the server puts it. */
export function ivRateSensitivity(legs:({points?:{rate_sensitivity_pct_points?:unknown}[]|null}|null|undefined)[]){
 let worst:number|null=null;
 for(const leg of legs||[]){
  for(const point of leg?.points||[]){
   const n=num(point?.rate_sensitivity_pct_points);
   if(n==null)continue;
   if(worst==null||Math.abs(n)>Math.abs(worst))worst=n;
  }
 }
 return worst;
}
/** How many readings the solver refused, and why, in the server's own sentences. A refusal is not a blank. */
export function ivRejectionLines(body?:{rejections?:Record<string,number>|null;
 reason_text?:Record<string,string>|null}|null){
 const counts=body?.rejections||{};
 const text=body?.reason_text||{};
 return Object.entries(counts).filter(([,n])=>num(n)!=null&&Number(n)>0)
  .sort((a,b)=>Number(b[1])-Number(a[1]))
  .map(([key,n])=>`${Math.round(Number(n))} reading${Math.round(Number(n))===1?'':'s'}: ${text[key]||key}`);
}
/** The model line: what solved it, how, and over what day count. */
export function ivMethodText(body?:{model?:string|null;method?:string|null;day_count?:string|null;
 expiry_time_ist?:string|null}|null){
 const model=String(body?.model||'').trim();
 if(!model)return 'The server did not name the model these were solved with.';
 const method=String(body?.method||'').trim();
 const days=String(body?.day_count||'').trim();
 const at=String(body?.expiry_time_ist||'').trim();
 return `Model: ${model}${method?` · ${method}`:''}${days?` · ${days}${at?` to ${at} IST on the expiry date`:''}`:''}.`;
}
/** The reason one reading carried no volatility. The server's sentence always wins; the map is only a floor for a
 *  bare code, and an unknown code is said as itself rather than swallowed. */
export function ivPointReason(point?:{reason?:string|null;reason_text?:string|null}|null,
 served?:Record<string,string>|null){
 const own=String(point?.reason_text||'').trim();
 if(own)return own;
 const key=String(point?.reason||'').trim();
 if(!key)return '';
 return String(served?.[key]||'').trim()||IV_REASONS[key]||`No volatility: the server gave the reason "${key}".`;
}
/** The latest reading that carried no volatility, so a dash on screen can print WHY it is a dash. */
export function ivLatestRefusal(points?:{iv?:unknown;reason?:string|null;reason_text?:string|null}[]|null,
 served?:Record<string,string>|null){
 const last=[...(points||[])].reverse().find(p=>num(p?.iv)==null&&(p?.reason||p?.reason_text));
 return last?ivPointReason(last,served):'';
}

// --- futures build-up, aligned -------------------------------------------------------------------------------------
/** The one line under the futures panel. Every figure is the server's `latest_*`, which is read off the last
 *  reading that carried one — never off a gap. */
export function futuresLine(body?:{latest_oi_vs_avg?:unknown;latest_basis?:unknown;latest_basis_pct?:unknown;
 latest_buildup_day?:string|null}|null){
 if(!body)return '';
 const buildup=servedBuildup(body.latest_buildup_day);
 const basis=basisPair(body.latest_basis,body.latest_basis_pct);
 // A basis is a futures price measured against a spot, so with no spot there is no basis to dash out — the
 // same rule the max-pain line works to, for the same reason: a bare dash in a sentence explains nothing.
 return `${oiVsAvgText(body.latest_oi_vs_avg)} · ${basis===DASH?BASIS_NO_SPOT:`basis ${basis}`}`
  +`${buildup===DASH?'':` · ${buildup} on the day`}.`;
}
/** Why a basis is missing. It is always the same reason: the store captured the contract but not the spot. */
export const BASIS_NO_SPOT='no spot was captured at this reading, so there is no basis';
export const BASIS_NO_SPOT_ROW='A basis needs a futures price and a spot at the same reading.';
/** A build-up label the server already wrote for the reader. "no data" is a state, not a label, and reads as one. */
export function servedBuildup(label?:string|null){
 const text=String(label||'').trim();
 if(!text||text==='no data')return DASH;
 return text;
}

// --- the screener, aligned -------------------------------------------------------------------------------------------
/** How wide the reading on screen was. This is what stands in place of a blank when nothing cleared the floors:
 *  a reading that scanned 255 rows across 216 underlyings and passed none of them is a very different thing from
 *  a reading that was never taken, and the reader is owed the difference. */
export function coverageText(body?:{scanned?:unknown;total?:unknown;returned?:unknown;
 coverage?:{underlyings?:unknown;rows?:unknown}|null}|null){
 if(!body)return '';
 const scanned=num(body.scanned),total=num(body.total),returned=num(body.returned);
 if(scanned==null&&total==null)return '';
 const names=num(body.coverage?.underlyings);
 const across=names==null?'':` across ${Math.round(names)} underlying${Math.round(names)===1?'':'s'}`;
 const kept=total==null?'':`${Math.round(total)} cleared the floors and the applied filters`;
 const shown=returned!=null&&total!=null&&returned<total?`, and this list carries ${Math.round(returned)} of them`:'';
 return `${scanned==null?'':`${Math.round(scanned).toLocaleString('en-IN')} row${Math.round(scanned)===1?'':'s'} at this 15-min reading${across}`}`
  +`${scanned!=null&&kept?'; ':''}${kept}${shown}.`;
}
/** The readings the store holds, newest first, as choices for the `at` control. Each says how wide it was, so a
 *  reader stepping back knows what they are stepping to. */
export type ReadingChoice={value:string;label:string;short:string;detail:string};
/** THIS SESSION's readings, newest first — not the newest N of them. The list the server sends runs back over
 *  several sessions, and a window of the newest dozen can miss the very readings that have rows: a session
 *  whose afternoon was rebuilt from candles carries almost nothing while its morning carries everything. A
 *  control that cannot reach the morning is a control that cannot answer the question the empty list raises. */
export function readingChoices(body?:{readings?:{at?:string;rows?:unknown;underlyings?:unknown}[]|null}|null,
 want=40):ReadingChoice[]{
 const all=body?.readings||[];
 const session=stamp(all[0]?.at)?.iso||'';
 const ofSession=session?all.filter(row=>stamp(row?.at)?.iso===session):all;
 return (ofSession.length?ofSession:all).slice(0,want).map(row=>{
  const at=String(row?.at||'');
  const s=stamp(at);
  const names=num(row?.underlyings);
  // BOTH forms are built here, from the stamp itself. The short one used to be the long one with its date
  // cut off by a regex at render time, which is a transform on copy standing between the words and the
  // reader — and a transform on copy is exactly where a sentence can be mangled with every check still green.
  return {value:at,label:s?`${s.date} · ${s.time}`:at,short:s&&s.time?s.time:at,
   detail:names==null?'':`${Math.round(names)} underlying${Math.round(names)===1?'':'s'} covered`};
 }).filter(c=>!!c.value);
}
/** The sentence under an empty screener. The server's own empty note first; then what the reading actually held,
 *  because "nothing cleared the floors" and "this reading holds almost nothing" are different facts. */
export function screenerEmptyDetail(body?:{empty_note?:string|null;scanned?:unknown;total?:unknown;
 coverage?:{underlyings?:unknown}|null;readings?:unknown}|null){
 const coverage=coverageText(body);
 const more=Array.isArray(body?.readings)&&(body?.readings as unknown[]).length>1
  ?' Earlier readings of this session are listed above; choosing one moves this whole block to it.':'';
 return `${coverage}${more}`.trim();
}

