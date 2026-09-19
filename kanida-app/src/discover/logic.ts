// Discover Strategies pure logic (docs/FALCON_DISCOVER_SPEC.md §3). No React; exercised by scripts/check-discover.cjs.
import {pct,searchText} from '../model';
import type {StrategyRow,StrategySummary} from '../strategies/types';
export type SortKey='low'|'avg'|'n'|'symbol';
export const SORT_LABELS:Record<SortKey,string>={low:'95% low',avg:'Avg',n:'n',symbol:'Symbol'};
export type ColumnsPref='auto'|'2'|'4';
export type GridMode='four'|'two'|'stack';
/** CARD_H: stacked (phone) cards. GRID_H: 4-across and 2×2 grid cards. */
export const CARD_H=420,GRID_H=460,COST_LINE='Costs 0.40% included · next-open entry';
export const HOLD_LABEL='Pattern hold-period history — not a tested exit rule',TESTED_LABEL='Tested exit rule · later-test (out-of-sample)';
const num=(v:number|null|undefined)=>v==null||!Number.isFinite(v)?null:v;
const desc=(a:number|null,b:number|null)=>a==null&&b==null?0:a==null?1:b==null?-1:b-a;
/** Sorted copy. Numbers descending with nulls last; ties → n desc → symbol. Symbol sorts A→Z. */
export function sortRows(rows:StrategyRow[],key:SortKey):StrategyRow[]{
 return [...rows].sort((x,y)=>{if(key==='symbol')return x.symbol.localeCompare(y.symbol)||x.timeframe.localeCompare(y.timeframe);const d=key==='n'?(y.n||0)-(x.n||0):desc(num(key==='low'?x.low_pct:x.avg_pct),num(key==='low'?y.low_pct:y.avg_pct));return d||(y.n||0)-(x.n||0)||x.symbol.localeCompare(y.symbol)});
}
export const isSortKey=(v:unknown):v is SortKey=>v==='low'||v==='avg'||v==='n'||v==='symbol';
/** Only small / missing samples carry a label in the row (spec §3.3). */
export function rowSampleNote(r:StrategyRow){return r.sample_label==='Small sample'||r.sample_label==='No history'?r.sample_label:'';}
export type StatusInput={phase:'idle'|'loading'|'done'|'error'|'stopped';universe:string;date:string;found:number;shown:number;revealing:boolean};
/** Status line text. Stored results never say "Scanning": only "Loading stored scan…" while the request is in flight or rows are still revealing. `short` = one-line card form (full text stays in the ⓘ popover and accessibility label). */
export function statusText(i:StatusInput,short=false){
 const d=short?i.date.replace(/\s+\d{4}$/,''):i.date;
 if(i.phase==='idle')return 'Choose a strategy to list stocks.';
 if(i.phase==='loading')return 'Loading stored scan…';
 if(i.phase==='stopped')return short?'Stopped before loading.':'Stopped before the stored scan loaded.';
 if(i.phase==='error')return short?'Results unavailable.':'Strategy results are unavailable right now.';
 if(!i.found)return short?`Stored · ${i.universe} · to ${d} · none match`:`No ${i.universe} stock matches this strategy on data to ${i.date}.`;
 if(i.revealing)return short?`Loading… ${i.shown} found`:`Loading stored scan… ${i.shown} found`;
 return short?`Stored · ${i.universe} · to ${d} · ${i.found} found`:`Stored scan · ${i.universe} · data to ${i.date} · ${i.found} found`;
}
/** One-line picker tag: found count + best 95% low (block and universe are implied by the page). */
export function pickerTag(st:StrategySummary){return `${st.found} found · 95% low ${pct(st.best_low_pct)}`;}
export const CHIP_ORDER=['Chart patterns','Bullish','Bearish','1H','4H','1D','1W'];
/** Chips come from registry tags (admin additions appear automatically): All · known tags in spec order · other tags A→Z · Recently used. */
export function pickerChips(list:StrategySummary[]){const tags=new Set(list.flatMap(x=>x.tags||[]));const known=CHIP_ORDER.filter(t=>tags.has(t)),rest=[...tags].filter(t=>!CHIP_ORDER.includes(t)).sort();return ['All',...known,...rest,'Recently used'];}
export function filterStrategies(list:StrategySummary[],query:string,chip:string,recent:string[]){
 const terms=searchText(query||'').split(' ').filter(Boolean);
 const out=list.filter(x=>(chip==='All'||(chip==='Recently used'?recent.includes(x.key):(x.tags||[]).includes(chip)))&&terms.every(q=>searchText(`${x.name} ${x.pattern_name} ${x.timeframe} ${x.side} ${(x.tags||[]).join(' ')}`).includes(q)));
 return chip==='Recently used'?out.sort((a,b)=>recent.indexOf(a.key)-recent.indexOf(b.key)):out;
}
export function pushRecent(recent:string[],key:string,cap=6){return [key,...(recent||[]).filter(k=>k!==key)].slice(0,cap);}
/** Registry defaults. A = default_slot A (else first by order). B = default_slot B, else the other strategy on A's side with the best 95% low and found > 0, else the next one. */
export function defaultSlots(list:StrategySummary[]):{A:string|null;B:string|null}{
 const on=[...list].filter(x=>x.enabled!==false).sort((a,b)=>a.order-b.order);
 const A=on.find(x=>x.default_slot==='A')?.key??on[0]?.key??null,a=on.find(x=>x.key===A);
 const others=on.filter(x=>x.key!==A),best=others.filter(x=>x.found>0&&(!a||x.side===a.side)).sort((x,y)=>desc(num(x.best_low_pct),num(y.best_low_pct)))[0];
 const B=others.find(x=>x.default_slot==='B')?.key??best?.key??others[0]?.key??null;
 return {A,B};
}
/** A stored choice wins while it still exists; null = user cleared the card; anything else → registry default. */
export function resolveSlot(stored:string|null|undefined,fallback:string|null,list:StrategySummary[]){if(stored===null)return null;if(stored&&list.some(x=>x.key===stored&&x.enabled!==false))return stored;return fallback;}
export function gridMode(width:number,columns:ColumnsPref):GridMode{if(width<760)return 'stack';if(columns==='4')return 'four';if(columns==='2')return 'two';return width>=1280?'four':'two';}
/** Reveal step per 25 ms tick: one row at a time, but the whole list finishes within ~1 s. */
export function revealStep(total:number){return Math.max(1,Math.ceil(total/40));}
export type Verdict='review'|'watch'|'pass';
export const VERDICT_TITLES:Record<Verdict,string>={review:'For review',watch:'Watch',pass:'Pass'};
/** Mirrors decision.ts: enough trades + positive avg after costs → For review; avg ≤ 0 or whole 95% band below zero → Pass; otherwise Watch. */
export function verdictFor(stats:any,minTrades:number):Verdict{
 const n=stats?.n||0,mean=num(stats?.expectancy_pct),ci=stats?.expectancy_ci95;
 if(n>=5&&Array.isArray(ci)&&num(ci[1])!=null&&ci[1]<0)return 'pass';
 if(n>=Math.max(1,minTrades)&&mean!=null)return mean<=0?'pass':'review';
 return 'watch';
}
export type BacktestView={basis:'tested_rule'|'hold_period_history'|'none';stats:any;trades:any[];label:string;rule:string};
/** Backtest card source (spec §3.6 + §8). Tested cell → later-test split stats + test trades. Otherwise the whole-history hold-period reference + reference trades, labelled as not a tested exit rule. */
export function backtestView(cell:any):BacktestView{
 if(cell?.status==='tested'&&(cell.splits?.test?.n||0)>0)return {basis:'tested_rule',stats:cell.splits.test,trades:(cell.trades||[]).filter((t:any)=>t.split==='test'),label:TESTED_LABEL,rule:cell.rule_description||''};
 if((cell?.reference?.n||0)>0)return {basis:'hold_period_history',stats:cell.reference,trades:cell.reference_trades||[],label:HOLD_LABEL,rule:cell.reference.description||''};
 return {basis:'none',stats:null,trades:[],label:'',rule:''};
}
/** Chronological trades → worst trade, average hold (bars) and the trade-by-trade cumulative sum of net returns (not a portfolio). */
export function tradeSummary(trades:any[]){
 const list=[...(trades||[])].filter(t=>Number.isFinite(t?.net_return_pct)).sort((a,b)=>String(a.entry_time||'').localeCompare(String(b.entry_time||'')));
 let run=0;const curve=list.map(t=>(run+=t.net_return_pct));
 const holds=list.map(t=>t.holding_bars).filter((v:any)=>Number.isFinite(v));
 return {list,curve,worst:list.length?Math.min(...list.map(t=>t.net_return_pct)):null,avgHold:holds.length?holds.reduce((a:number,b:number)=>a+b,0)/holds.length:null,total:list.length?run:null};
}
/** Card form of holdText: "10.0d", "3.0w", "6.5 × 1H". */
export function holdShort(bars:number|null,timeframe:string){if(bars==null||!Number.isFinite(bars))return '—';const v=bars.toFixed(1);return timeframe==='1D'?`${v}d`:timeframe==='1W'?`${v}w`:`${v} × ${timeframe}`;}
export function holdText(bars:number|null,timeframe:string){if(bars==null||!Number.isFinite(bars))return '—';const v=bars.toFixed(1);return timeframe==='1D'?`${v} trading days`:timeframe==='1W'?`${v} weeks`:`${v} × ${timeframe} candles`;}
