// Section 4 — Index dashboard (§4 card 4): NIFTY / BANKNIFTY / FINNIFTY, their PCR, max pain and how open interest
// moved through the day, as one table row each. The day's shift stays a drawn sparkline in its own column because
// §4.4 asks for it; a mark that was not captured is a gap in that line, never a straight line drawn across it.
import React,{useMemo,useState} from 'react';
import {View} from 'react-native';
import Svg,{Path,Line} from 'react-native-svg';
import {C,T} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {WidgetFrame,stateOf} from './frame';
import {Table,Cell,type Column,type TableItem} from './Table';
import {clock,compact,dteText,linePath,maxPainText,nextSort,pcrText,price,scaleSeries,shortDate,sortRows,
 type SortState} from './logic';
import type {IndexRow,Indices,ChartTarget} from './types';
const SPARK_W=118,SPARK_H=30;
const VALUE:Record<string,(row:IndexRow)=>unknown>={symbol:r=>r.underlying,spot:r=>r.spot,pcr_oi:r=>r.pcr_oi,
 pcr_vol:r=>r.pcr_volume,max_pain:r=>r.max_pain_strike,expiry:r=>r.expiry,dte:r=>r.days_to_expiry,
 ce_oi:r=>r.total_ce_oi,pe_oi:r=>r.total_pe_oi,time:r=>r.captured_at};
/** Total option OI through the day: CE and PE on ONE scale so they are comparable, gaps preserved. */
function OiSpark({row}:{row:IndexRow}){
 const points=row.series||[];
 const ce=scaleSeries(points.map(p=>({t:p.captured_at,price:p.total_ce_oi,oi:null})),'price',SPARK_W,SPARK_H);
 const pe=scaleSeries(points.map(p=>({t:p.captured_at,price:p.total_pe_oi,oi:null})),'price',SPARK_W,SPARK_H);
 if(!ce&&!pe)return <T numberOfLines={2} style={{fontSize:9,lineHeight:12,color:C.muted}}>Needs two captured readings</T>;
 const lo=Math.min(ce?.lo??Infinity,pe?.lo??Infinity),hi=Math.max(ce?.hi??-Infinity,pe?.hi??-Infinity),span=(hi-lo)||1;
 const map=(values:(number|null)[])=>{const step=values.length>1?SPARK_W/(values.length-1):0;
  return linePath({lo,hi,points:values.map((v,i)=>v==null?null:{x:i*step,y:SPARK_H-((v-lo)/span)*SPARK_H,i})})};
 return <Svg width={SPARK_W} height={SPARK_H}
  accessibilityLabel={`${row.underlying} option open interest from ${clock(points[0]?.captured_at)} to ${clock(points[points.length-1]?.captured_at)}`}>
  <Line x1={0} x2={SPARK_W} y1={SPARK_H-0.5} y2={SPARK_H-0.5} stroke={C.line} strokeWidth={1}/>
  <Path d={map(points.map(p=>p.total_ce_oi))} stroke="#F17D87" strokeWidth={1.3} fill="none"/>
  <Path d={map(points.map(p=>p.total_pe_oi))} stroke={C.green} strokeWidth={1.3} fill="none"/>
 </Svg>;
}
export type IndexWidgetProps={seq:number;target:ChartTarget|null;onTarget:(t:ChartTarget)=>void;onExpand?:()=>void;
 expanded?:boolean;onClose?:()=>void;pinFirst?:boolean;style?:any};
export function IndexWidget({seq,target,onTarget,onExpand,expanded,onClose,pinFirst,style}:IndexWidgetProps){
 const read=useDerivativeRead<Indices>('/api/derivatives/indices',seq);
 const body=read.data,state=stateOf(read,'No index snapshot has been captured yet.');
 const [sort,setSort]=useState<SortState>(null);
 const rows=body?.rows||[];
 const columns:Column<IndexRow>[]=useMemo(()=>[
  {key:'symbol',label:'Symbol',width:96,value:VALUE.symbol,render:r=><Cell text={r.underlying} bold size={12}/>},
  {key:'spot',label:'Spot',width:88,align:'right',value:VALUE.spot,render:r=><Cell text={price(r.spot)}/>},
  {key:'pcr_oi',label:'PCR (OI)',width:70,align:'right',value:VALUE.pcr_oi,render:r=><Cell text={pcrText(r.pcr_oi)} bold/>},
  {key:'pcr_vol',label:'PCR (vol)',width:74,align:'right',value:VALUE.pcr_vol,render:r=><Cell text={pcrText(r.pcr_volume)}/>},
  {key:'max_pain',label:'Max pain',width:150,grow:true,value:VALUE.max_pain,
   render:r=><Cell text={maxPainText(r)} color={C.muted}/>},
  {key:'expiry',label:'Exp. date',width:72,value:VALUE.expiry,render:r=><Cell text={shortDate(r.expiry)||'—'} color={C.muted}/>},
  {key:'dte',label:'DTE',width:42,align:'right',value:VALUE.dte,
   render:r=><Cell text={r.days_to_expiry==null?'—':String(r.days_to_expiry)}/>},
  {key:'ce_oi',label:'CE OI',width:66,align:'right',value:VALUE.ce_oi,render:r=><Cell text={compact(r.total_ce_oi)} color="#F17D87"/>},
  {key:'pe_oi',label:'PE OI',width:66,align:'right',value:VALUE.pe_oi,render:r=><Cell text={compact(r.total_pe_oi)} color={C.green}/>},
  {key:'spark',label:'OI through the day',width:SPARK_W+12,render:r=><OiSpark row={r}/>},
  {key:'time',label:'Time',width:52,value:VALUE.time,render:r=><Cell text={clock(r.captured_at)} color={C.muted}/>},
 ],[]);
 const items=useMemo(():TableItem<IndexRow>[]=>{
  const list=sort?sortRows(rows,VALUE[sort.key]||(()=>null),sort.dir):rows;
  return list.map(row=>({kind:'row' as const,key:row.underlying,row}));
 },[rows,sort]);
 const missingIndices=rows.filter(r=>!r.captured).map(r=>r.underlying);
 return <WidgetFrame name="Index dashboard" subtitle="NIFTY · BANKNIFTY · FINNIFTY" body={body} state={state}
  onRefresh={read.reload} onExpand={onExpand} expanded={expanded} onClose={onClose} style={style}
  note={<>
   <T style={{fontSize:10,lineHeight:14,color:C.muted}}>
    PCR is put open interest divided by call open interest at this 15-min reading, and the volume PCR the same for the day&apos;s
    volume (§3.5). Max pain is computed from the open interest standing right now (§3.6). Both describe the book as
    captured; neither is a forecast.
   </T>
   {!!missingIndices.length&&<T style={{fontSize:10,lineHeight:14,color:C.amber}}>
    Not captured yet, so shown as dashes rather than zeros: {missingIndices.join(', ')}.
   </T>}
  </>}>
  <Table label="Index dashboard" columns={columns} items={items} sort={sort} pinFirst={pinFirst} rowHeight={42}
   onSort={key=>setSort(s=>nextSort(s,key))}
   onRowPress={row=>onTarget({underlying:row.underlying,instrumentToken:null,label:row.underlying,
    detail:`PCR ${pcrText(row.pcr_oi)} · max pain ${maxPainText(row)}`})}
   selected={row=>target?.underlying===row.underlying&&!target?.instrumentToken}
   rowLabel={row=>row.captured
    ?`${row.underlying}. Spot ${price(row.spot)}. Put-call ratio by open interest ${pcrText(row.pcr_oi)}, by volume ${pcrText(row.pcr_volume)}. Max pain ${maxPainText(row)}. Captured at ${clock(row.captured_at)}`
    :`${row.underlying}. Not captured yet — this index has no snapshot in the store, so no number is shown for it`}/>
 </WidgetFrame>;
}
