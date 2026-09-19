// Section 1 — Unusual activity (docs/DERIVATIVES_SPEC.md §3.2–§3.4, §4 card 1) as the benchmark's wide dense table:
// one row per strike that cleared the liquidity floors, the §3 roll-up kept as a band over each underlying's rows.
//
// Every honesty rule of the card it replaces still holds: the floors are the server's and are printed underneath, a
// volume reading without three sessions behind it is the words "no baseline" (§3.2), the 15-minute and day-on-day
// build-up labels are shown separately and never merged (§3.1), and a number that was not captured is a dash.
import React,{useMemo,useState} from 'react';
import {View} from 'react-native';
import {C,Icon,s} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {WidgetFrame,stateOf,tone,buildupColor,typeColor} from './frame';
import {Table,Cell,type Column,type TableItem} from './Table';
import {buildupLabel,buildupTone,clock,compact,contractSummary,crore,dteText,flattenUnusual,groupSummary,
 nextSort,optionTone,price,ratio,rowKey,rulesToFilters,shortDate,signedUnits,sortRows,strike as strikeText,
 units,unusualQuery,volumeRatioShort,volumeToOi,volumeToOiHot,type FilterRule,type SortState} from './logic';
import type {ContractRow,Unusual,ChartTarget} from './types';
/** The captured value behind each column, used by the header sort (and nothing else). */
const VALUE:Record<string,(row:ContractRow)=>unknown>={
 time:r=>r.captured_at,symbol:r=>r.underlying,summary:r=>r.tradingsymbol,expiry:r=>r.expiry,
 dte:r=>r.days_to_expiry,strike:r=>r.strike,spot:r=>r.spot,oi:r=>r.oi,oi_chg:r=>r.oi_change_day,
 volume:r=>r.volume,vol_oi:r=>r.volume_to_oi,premium:r=>r.premium_cr,buildup:r=>r.buildup_day,
};
export type UnusualWidgetProps={rules:FilterRule[];rulesLine:string;seq:number;target:ChartTarget|null;
 onTarget:(t:ChartTarget)=>void;onCustomize:()=>void;onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;
 pinFirst?:boolean;style?:any};
export function UnusualWidget({rules,rulesLine,seq,target,onTarget,onCustomize,onExpand,expanded,onClose,
 pinFirst,style}:UnusualWidgetProps){
 const read=useDerivativeRead<Unusual>(`/api/derivatives/unusual${unusualQuery(rulesToFilters(rules||[]))}`,seq);
 const body=read.data;
 const state=stateOf(read,'No contract clears the liquidity floors at this 15-min reading.');
 const [sort,setSort]=useState<SortState>(null);
 const required=body?.baseline_sessions_required??3;
 const groups=body?.rows||[];
 const flat=useMemo(()=>flattenUnusual(groups),[groups]);
 const pick=(row:ContractRow)=>onTarget({underlying:row.underlying,instrumentToken:row.instrument_token,
  label:row.tradingsymbol||row.underlying,
  detail:`${strikeText(row.strike)} ${row.instrument_type} · ${dteText(row.days_to_expiry)}`});
 const columns:Column<ContractRow>[]=useMemo(()=>[
  {key:'time',label:'Time',width:50,value:VALUE.time,render:r=><Cell text={clock(r.captured_at)} color={C.muted}/>},
  {key:'symbol',label:'Symbol',width:94,value:VALUE.symbol,filter:onCustomize,
   render:r=><Cell text={r.underlying||'—'} bold size={12}/>},
  {key:'summary',label:'Summary',width:150,grow:true,value:VALUE.summary,filter:onCustomize,
   render:r=><Cell text={contractSummary(r)} color={typeColor(optionTone(r.instrument_type))} bold size={12}/>},
  {key:'expiry',label:'Exp. date',width:72,value:VALUE.expiry,filter:onCustomize,
   render:r=><Cell text={shortDate(r.expiry)||'—'} color={C.muted}/>},
  {key:'dte',label:'DTE',width:42,align:'right',value:VALUE.dte,filter:onCustomize,
   render:r=><Cell text={r.days_to_expiry==null?'—':String(r.days_to_expiry)}/>},
  {key:'strike',label:'Strike',width:72,align:'right',value:VALUE.strike,render:r=><Cell text={strikeText(r.strike)}/>},
  {key:'spot',label:'Spot',width:76,align:'right',value:VALUE.spot,render:r=><Cell text={price(r.spot)} color={C.muted}/>},
  {key:'oi',label:'OI',width:62,align:'right',value:VALUE.oi,render:r=><Cell text={compact(r.oi)}/>},
  {key:'oi_chg',label:'OI chg',width:72,align:'right',value:VALUE.oi_chg,
   render:r=><Cell text={signedUnits(r.oi_change_day)} color={tone(r.oi_change_day)}/>},
  {key:'volume',label:'Volume',width:78,align:'right',value:VALUE.volume,
   render:r=><View style={{alignItems:'flex-end'}}>
    <Cell text={compact(r.volume)}/><Cell text={volumeRatioShort(r,required)} color={C.muted} size={9}/></View>},
  {key:'vol_oi',label:'Vol/OI',width:60,align:'right',value:VALUE.vol_oi,
   render:r=><View style={[s.row,{gap:3}]}>
    {volumeToOiHot(r)&&<Icon name="alert-circle" size={11} color={C.amber}/>}
    <Cell text={ratio(r.volume_to_oi)} color={volumeToOiHot(r)?C.amber:C.ink}/></View>},
  {key:'premium',label:'Premium ₹cr',width:76,align:'right',value:VALUE.premium,filter:onCustomize,
   render:r=><Cell text={crore(r.premium_cr)} bold/>},
  {key:'buildup',label:'Build-up',width:104,value:VALUE.buildup,render:r=><View>
   <Cell text={buildupLabel(r.buildup_day)} color={buildupColor(buildupTone(r.buildup_day))}/>
   <Cell text={`15m ${buildupLabel(r.buildup_15m)}`} color={C.muted} size={9}/></View>},
 ],[onCustomize,required]);
 // Unsorted, the table keeps the §3 roll-up as a band over each underlying's strikes. A sort runs across every row,
 // so the bands come off and the note says so rather than leaving a roll-up the order no longer matches.
 const items=useMemo(():TableItem<ContractRow>[]=>{
  if(sort){
   const get=VALUE[sort.key]||(()=>null);
   return sortRows(flat,get,sort.dir).map((row,i)=>({kind:'row' as const,key:rowKey(row,i),row}));
  }
  const out:TableItem<ContractRow>[]=[];
  for(const group of groups){
   out.push({kind:'group',key:`g-${group.underlying}`,label:group.underlying,detail:groupSummary(group)});
   for(const row of group.strikes||[])out.push({kind:'row',key:rowKey(row,group.underlying),row});
  }
  return out;
 },[sort,flat,groups]);
 const note=sort
  ?`${flat.length} strikes, sorted by ${columns.find(c=>c.key===sort.key)?.label||sort.key}. Sorting re-orders the rows the pilot returned at this 15-min reading, it does not ask for different ones, so the roll-up bands are off.`
  :`${flat.length} strikes across ${groups.length} underlying${groups.length===1?'':'s'} clear the floors, largest premium traded first.`;
 return <WidgetFrame name="Unusual activity" subtitle={rulesLine} body={body} state={state} onRefresh={read.reload}
  filterCount={(rules||[]).length} onCustomize={onCustomize} onExpand={onExpand} expanded={expanded} onClose={onClose}
  showsSignals={['premium_cr','volume_ratio','volume_to_oi','buildup_day','buildup_15m','oi_change_day','spot']}
  note={note} style={style} emptyDetail="A contract is listed only when it clears all three floors below.">
  <Table label="Unusual activity" columns={columns} items={items} sort={sort} pinFirst={pinFirst} rowHeight={36}
   onSort={key=>setSort(s=>nextSort(s,key))} onRowPress={pick}
   selected={row=>row.instrument_token!=null&&target?.instrumentToken===row.instrument_token}
   rowLabel={row=>`${row.tradingsymbol||row.underlying}. ${contractSummary(row)}. ${crore(row.premium_cr)} traded. ${buildupLabel(row.buildup_day)} on the day, ${buildupLabel(row.buildup_15m)} over the last 15 minutes. Volume ${volumeRatioShort(row,required)} of its own median, ${units(row.volume)} traded. Volume to open interest ${volumeToOi(row)}`}/>
 </WidgetFrame>;
}
