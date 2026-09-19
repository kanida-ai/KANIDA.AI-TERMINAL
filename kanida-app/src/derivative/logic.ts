// Pure formatting and card wording for the Derivative tab (docs/DERIVATIVES_SPEC.md §3-§5). No React, no fetch:
// scripts/check-derivative.cjs evaluates this file on its own.
//
// Three rules decide every function here:
//  1. A number that was not captured is a DASH, never a zero (§5).
//  2. A ratio without at least `baseline_sessions_required` sessions behind it is the words "no baseline",
//     never a number (§3.2) - and the UI never has to remember that, because the server already sends
//     volume_ratio: null with volume_baseline: 'none' and this file re-checks both.
//  3. Nothing is phrased as a prediction. Every line describes a state and says when it was captured (§5).
import type {ChainRow,ContractRow,Envelope,Floors,GridDirection,GridPoint,GridPriceDirection,GridSlot,IndexRow,
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
export type FilterColumn='underlying'|'expiry'|'optionType'|'dte'|'premium'|'watchlist';
export type FilterOperator='is'|'is_not'|'gt'|'lt'|'in_watchlist';
export type FilterRule={column:FilterColumn;operator:FilterOperator;value:string};
export const OPERATOR_LABELS:Record<FilterOperator,string>={is:'is',is_not:'is not',gt:'greater than',
 lt:'less than',in_watchlist:'is in watch list'};
/** The columns the popup offers, each with ONLY the operators the server can answer, and the parameter it becomes. */
export const FILTER_COLUMNS:{key:FilterColumn;label:string;operators:FilterOperator[];param:string;unit?:string}[]=[
 {key:'underlying',label:'Symbol',operators:['is'],param:'underlying'},
 {key:'expiry',label:'Exp. date',operators:['is'],param:'expiry'},
 {key:'optionType',label:'Type',operators:['is','is_not'],param:'option_type'},
 {key:'dte',label:'DTE',operators:['lt'],param:'max_dte',unit:'days'},
 {key:'premium',label:'Premium',operators:['gt'],param:'min_premium_cr',unit:'₹ cr'},
 {key:'watchlist',label:'Watch list',operators:['in_watchlist'],param:'watchlist'},
];
export const filterColumn=(key:unknown)=>FILTER_COLUMNS.find(c=>c.key===key)||null;
export const columnLabel=(key:unknown)=>filterColumn(key)?.label||String(key||'');
/** Day counts offered for "DTE less than N". Days-to-expiry is a whole number, so "< N" is exactly `max_dte = N-1`. */
export const DTE_VALUES=[1,3,7,15,31];
/** ₹ crore steps offered for "Premium greater than V". The §3 floor always applies underneath. */
export const PREMIUM_VALUES=[2,5,10,25,50];
const RULE_VALUE:Record<FilterColumn,(v:string)=>boolean>={
 underlying:v=>/^[A-Z0-9&._-]{1,40}$/.test(v),
 expiry:v=>/^\d{4}-\d{2}-\d{2}$/.test(v),
 optionType:v=>v==='CE'||v==='PE',
 dte:v=>/^\d{1,3}$/.test(v)&&Number(v)>=1&&Number(v)<=400,
 premium:v=>/^\d{1,6}(\.\d{1,2})?$/.test(v)&&Number(v)>=0&&Number(v)<=100000,
 watchlist:v=>/^[a-z][a-z0-9_-]{0,19}$/.test(v),
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
 return `At the money: ${at}, from a captured spot of ${spot}${body.expiry?` in the ${body.expiry} expiry`:''}.${window}`;
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
/** "8 days to expiry · 15-min candles · 26 stored" — how far the contract is from expiry, what is drawn, and
 *  how much of it there is. A part that was not served is left out rather than guessed. */
export function futuresChartSubtitle(body?:{contract?:FuturesContractRef|null;bars?:unknown;
 candles?:Candle[]|null}|null,intervalKey:string=FUTURES_DEFAULT_INTERVAL){
 const parts:string[]=[];
 const dte=dteText(body?.contract?.days_to_expiry);
 if(dte!==DASH)parts.push(dte);
 const choice=FUTURES_INTERVALS.find(c=>c.key===intervalKey);
 if(choice)parts.push(choice.label);
 const n=futuresBars(body);
 if(n)parts.push(`${n} stored`);
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
/** The one sentence that keeps a windowed chart honest, and '' when the whole series is on screen. */
export function candleWindowText(window?:CandleWindow|null){
 if(!window?.windowed)return '';
 return `Showing the latest ${window.shown} of ${window.total} candles in this series; the panel is too narrow for all of them.`;
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
/** A few x labels taken from candles that were actually stored, evenly spaced across the ones there are. */
export function candleAxisTimes(candles?:Candle[]|null,intervalKey:string=FUTURES_DEFAULT_INTERVAL,want=4){
 const marks=(candles||[]).map((c,i)=>({i,label:candleAxisLabel(c?.at,intervalKey)}))
  .filter(m=>m.label!==DASH&&m.label!=='');
 if(marks.length<=want)return marks;
 const step=(marks.length-1)/(want-1);
 const out:{i:number;label:string}[]=[];
 for(let k=0;k<want;k++){
  const mark=marks[Math.round(k*step)];
  if(mark&&!out.some(m=>m.i===mark.i))out.push(mark);
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
 const first=candleAxisLabel(candles[0]?.at,intervalKey),last=candleAxisLabel(candles[candles.length-1]?.at,intervalKey);
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
