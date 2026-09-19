// Section 5 — Futures OI build-up across underlyings (§3.7, §4 card 5): the front futures contract per underlying
// with its day-on-day build-up label, open interest as a share of its own 20-day average, and basis (futures − spot).
// A basis needs BOTH prices; with one of them missing the cell is a dash, never half a number.
import React,{useMemo,useState} from 'react';
import {C} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {WidgetFrame,stateOf,tone,buildupColor} from './frame';
import {Table,Cell,type Column,type TableItem} from './Table';
import {basisText,buildupLabel,buildupTone,clock,compact,contractSummary,dteText,nextSort,oiShareText,price,
 rowKey,shortDate,signed,signedUnits,sortRows,type SortState} from './logic';
import type {ContractRow,Futures,ChartTarget} from './types';
const VALUE:Record<string,(row:ContractRow)=>unknown>={time:r=>r.captured_at,symbol:r=>r.underlying,
 summary:r=>r.tradingsymbol,expiry:r=>r.expiry,dte:r=>r.days_to_expiry,price:r=>r.last_price,
 chg:r=>r.price_change_day_pct,oi:r=>r.oi,oi_chg:r=>r.oi_change_day,oi_avg:r=>r.oi_vs_20d_avg,basis:r=>r.basis,
 buildup:r=>r.buildup_day};
export type FuturesWidgetProps={underlying:string;watchlist:string;seq:number;target:ChartTarget|null;
 onTarget:(t:ChartTarget)=>void;filterCount?:number;onCustomize?:()=>void;onExpand?:()=>void;expanded?:boolean;
 onClose?:()=>void;pinFirst?:boolean;style?:any};
export function FuturesWidget({underlying,watchlist,seq,target,onTarget,filterCount,onCustomize,onExpand,expanded,
 onClose,pinFirst,style}:FuturesWidgetProps){
 const query=[underlying?`underlying=${encodeURIComponent(underlying)}`:'',
  watchlist&&watchlist!=='all'?`watchlist=${encodeURIComponent(watchlist)}`:''].filter(Boolean).join('&');
 const read=useDerivativeRead<Futures>(`/api/derivatives/futures${query?`?${query}`:''}`,seq);
 const body=read.data,state=stateOf(read,'No futures contract has been captured yet.');
 const [sort,setSort]=useState<SortState>(null);
 const rows=body?.rows||[];
 const columns:Column<ContractRow>[]=useMemo(()=>[
  {key:'time',label:'Time',width:50,value:VALUE.time,render:r=><Cell text={clock(r.captured_at)} color={C.muted}/>},
  {key:'symbol',label:'Symbol',width:100,value:VALUE.symbol,filter:onCustomize,
   render:r=><Cell text={r.underlying||'—'} bold size={12}/>},
  {key:'summary',label:'Summary',width:130,grow:true,value:VALUE.summary,
   render:r=><Cell text={contractSummary(r)} size={12}/>},
  {key:'expiry',label:'Exp. date',width:72,value:VALUE.expiry,filter:onCustomize,
   render:r=><Cell text={shortDate(r.expiry)||'—'} color={C.muted}/>},
  {key:'dte',label:'DTE',width:42,align:'right',value:VALUE.dte,filter:onCustomize,
   render:r=><Cell text={r.days_to_expiry==null?'—':String(r.days_to_expiry)}/>},
  {key:'price',label:'Price',width:84,align:'right',value:VALUE.price,render:r=><Cell text={price(r.last_price)}/>},
  {key:'chg',label:'Chg %',width:62,align:'right',value:VALUE.chg,
   render:r=><Cell text={signed(r.price_change_day_pct)} color={tone(r.price_change_day_pct)}/>},
  {key:'oi',label:'OI',width:62,align:'right',value:VALUE.oi,render:r=><Cell text={compact(r.oi)}/>},
  {key:'oi_chg',label:'OI chg',width:74,align:'right',value:VALUE.oi_chg,
   render:r=><Cell text={signedUnits(r.oi_change_day)} color={tone(r.oi_change_day)}/>},
  {key:'oi_avg',label:'OI vs 20-day',width:112,align:'right',value:VALUE.oi_avg,
   render:r=><Cell text={oiShareText(r.oi_vs_20d_avg)} color={C.muted}/>},
  {key:'basis',label:'Basis',width:78,align:'right',value:VALUE.basis,
   render:r=><Cell text={basisText(r.basis)} color={tone(r.basis)}/>},
  {key:'buildup',label:'Build-up',width:104,value:VALUE.buildup,
   render:r=><Cell text={buildupLabel(r.buildup_day)} color={buildupColor(buildupTone(r.buildup_day))}/>},
 ],[onCustomize]);
 const items=useMemo(():TableItem<ContractRow>[]=>{
  const list=sort?sortRows(rows,VALUE[sort.key]||(()=>null),sort.dir):rows;
  return list.map((row,i)=>({kind:'row' as const,key:rowKey(row,i),row}));
 },[rows,sort]);
 return <WidgetFrame name="Futures OI build-up" subtitle={`${body?.total??0} front contracts`} body={body}
  state={state} onRefresh={read.reload} filterCount={filterCount} onCustomize={onCustomize} onExpand={onExpand}
  expanded={expanded} onClose={onClose} style={style}
  showsSignals={['buildup_day','oi_change_day','oi_vs_20d_avg','basis','price_change_day_pct']}
  note="Build-up compares this contract's price change with its open-interest change over the same day-on-day window (§3.1). Basis is futures minus the captured spot.">
  <Table label="Futures build-up" columns={columns} items={items} sort={sort} pinFirst={pinFirst}
   onSort={key=>setSort(s=>nextSort(s,key))}
   onRowPress={row=>onTarget({underlying:row.underlying,instrumentToken:row.instrument_token,
    label:row.tradingsymbol||row.underlying,detail:`Futures · ${dteText(row.days_to_expiry)}`})}
   selected={row=>row.instrument_token!=null&&target?.instrumentToken===row.instrument_token}
   rowLabel={row=>`${row.underlying} futures. ${buildupLabel(row.buildup_day)} on the day. Price ${price(row.last_price)}, change ${signed(row.price_change_day_pct)}. Open interest ${compact(row.oi)}, change ${signedUnits(row.oi_change_day)}, ${oiShareText(row.oi_vs_20d_avg)}. Basis ${basisText(row.basis)}. ${dteText(row.days_to_expiry)}`}/>
 </WidgetFrame>;
}
