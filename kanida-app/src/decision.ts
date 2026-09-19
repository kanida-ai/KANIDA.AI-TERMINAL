import {Match,Filters,histories,initialFilters} from './model';
import {MIN_TRADES_DEFAULT,DATA_STALE_DAYS,TIMEFRAMES} from './constants';
export type Verdict='review'|'watch'|'pass';
// Shared evidence constants. Sample bands: <10 small, 10–29 moderate, ≥30 larger.
// MIN_TRADES_DEFAULT (10) and DATA_STALE_DAYS (3) live in constants.ts; re-exported so existing './decision' imports keep working.
export {MIN_TRADES_DEFAULT,DATA_STALE_DAYS};
export const SAMPLE_MODERATE_MIN=10,SAMPLE_LARGER_MIN=30;
export const ALL_FRAMES=TIMEFRAMES;
export type DiscoverFilters=Filters&{frames?:string[]};
export const verdictTitles:Record<Verdict,string>={review:'For review',watch:'Watch',pass:'Pass'};
export function sampleSize(n?:number|null){const v=n||0;return v>=SAMPLE_LARGER_MIN?{label:'Larger sample',tone:'green'}:v>=SAMPLE_MODERATE_MIN?{label:'Moderate sample',tone:'neutral'}:{label:'Small sample',tone:'amber'};}
// Safe Discover defaults: minimum trade count on, plus the timeframes chosen at onboarding.
export function discoverDefaults(prefs?:string[]):DiscoverFilters{
 const frames=(Array.isArray(prefs)?prefs:[]).filter(tf=>ALL_FRAMES.includes(tf));
 if(frames.length===1)return {...initialFilters,minimum:MIN_TRADES_DEFAULT,timeframe:frames[0],frames:[]};
 return {...initialFilters,minimum:MIN_TRADES_DEFAULT,timeframe:'All',frames:frames.length&&frames.length<ALL_FRAMES.length?frames:[]};
}
export function frameMatch(m:Match,f:DiscoverFilters){return f.timeframe!=='All'||!f.frames?.length||f.frames.includes(m.timeframe);}
// --- the sample-size / return-range filters, where there is no history to count -----------------------------
// Both read a match's LEGACY backtest history. A research detection carries none by design - its evidence is
// its own research card, looked up by identity - so they cannot run on one: applying them would delete every
// detection rather than filter any. They are therefore NOT applied there, and every control that shows them
// says so instead of reading as if a filter were on. Same fact the scanner reports on /api/matches as
// `history_screen.applied: false` (market_scanner/performance.py).
export function historyScreenOff(patternSet?:string|null){return patternSet==='research';}
/** Short, plain line for a single control that would otherwise claim a sample-size filter is active. */
export const NO_HISTORY_SCREEN_NOTE='These setups have no past trades to count. The sample-size filter is off. Each setup’s evidence is on its own card.';
/** The same fact said once for BOTH history-based controls, where they sit together (the Filters sheet). */
export const NO_HISTORY_SCREEN_PAIR_NOTE='These setups have no past trades to count, so the return-range and sample-size filters are off. Each setup’s evidence is on its own card.';
export const NO_HISTORY_SCREEN_LABEL='No sample-size filter';
/** The filters actually applied to the list: the history-based ones are cleared when they cannot run. */
export function appliedFilters<T extends Filters>(f:T,off:boolean):T{return off?{...f,minimum:0,band:''}:f;}
// Calendar days between the stored-market end date and today (local). null when unknown.
export function dataAgeDays(date?:string,now=new Date()){const m=/^(\d{4})-(\d{2})-(\d{2})/.exec(date||'');if(!m)return null;const ist=new Date(now.getTime()+330*60000);return Math.max(0,Math.round((Date.UTC(ist.getUTCFullYear(),ist.getUTCMonth(),ist.getUTCDate())-Date.UTC(+m[1],+m[2]-1,+m[3]))/86400000));} // IST market date, matching the server
export function dataAgeText(days:number|null){return days==null?'Data age unknown':days===0?'Data from today':`Data ${days} ${days===1?'day':'days'} old`;}
export function isStale(days:number|null,flag?:boolean){return !!flag||days==null||days>DATA_STALE_DAYS;}
export function decision(m:Match,f:Filters,side?:string){
 const h=(side?m.history.find(h=>h.side===side):undefined)||histories(m,f)[0]||m.history[0];const stats=h?.reference;const mean=stats?.expectancy_pct;
 let verdict:Verdict='watch';
 let why='There is not enough stock-specific history to judge the outcome.';
 if(stats&&stats.n>=Math.max(1,f.minimum)&&mean!=null){
  if(mean<=0){verdict='pass';why='The past average did not cover the assumed trading costs.';}
  else if(m.direction==='neutral'){why='The structure is present. Its breakout direction is still unresolved.';}
  else {verdict='review';why=`A positive past average on this stock, with ${stats.n} historical trades.`;}
 }
 const later=h?.test;if((later?.n||0)>=5&&later?.expectancy_ci95&&later.expectancy_ci95[1]<0){verdict='pass';why='The selected exit rule lost money on later data after costs.';}
 const n=stats?.n||0;
 return {verdict,why,history:h,stats,title:verdictTitles[verdict],sample:sampleSize(n),
  next:!m.current?'Wait for a fresh completed candle. Recheck the pattern before any entry.':m.direction==='neutral'?'Wait for a confirmed direction before preparing the trade.':m.state==='setup'?'Recheck the boundary and confirmation on the next completed candle.':'Review the historical evidence and prepare an entry and exit plan.',
  caution:n<SAMPLE_MODERATE_MIN?'Small historical sample. A few outcomes can change the average.':n<SAMPLE_LARGER_MIN?'Moderate historical sample. The average can still shift materially.':'A past average describes the recorded trades; it does not establish the next trade’s outcome.'};
}
