// Section 3 — OI by strike (§4 card 3): CE against PE at each strike of the chosen expiry. The benchmark's shape for
// a section like this is a chart tile beside its numbers, so the one read feeds two widgets — the numeric table on
// the left and the bar chart on the right. The bars are the STORED open interest; nothing is smoothed, interpolated
// or projected, and max pain and the captured spot are marked where they actually fall.
import React,{useMemo,useState} from 'react';
import {View,ScrollView} from 'react-native';
import Svg,{Rect,Line,Text as SvgText} from 'react-native-svg';
import {C,T,s} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {Section,WidgetFrame,stateOf,tone,buildupColor} from './frame';
import {Table,Cell,type Column,type TableItem} from './Table';
import {buildupLabel,buildupTone,compact,dteText,maxPainBasis,nearestStrikeIndex,nextSort,oiPeak,price,
 signedUnits,sortRows,strike as strikeText,type SortState} from './logic';
import type {OiByStrike,StrikeOi,ChartTarget} from './types';
const CE_COLOR='#F17D87',PE_COLOR='#39E5A3',PAD_BOTTOM=18;
const VALUE:Record<string,(row:StrikeOi)=>unknown>={strike:r=>r.strike,ce_oi:r=>r.ce_oi,ce_chg:r=>r.ce_oi_change_day,
 ce_buildup:r=>r.ce_buildup_day,pe_oi:r=>r.pe_oi,pe_chg:r=>r.pe_oi_change_day,pe_buildup:r=>r.pe_buildup_day};
export type OiByStrikeSectionProps={underlying:string;expiry:string;seq:number;onTarget:(t:ChartTarget)=>void;
 stacked?:boolean;pinFirst?:boolean;filterCount?:number;onCustomize?:()=>void;height:number;chartHeight:number;
 hidden?:Record<string,boolean>;onHide?:(key:string)=>void;expanded?:string;onExpand?:(key:string)=>void};
export function OiByStrikeSection({underlying,expiry,seq,onTarget,stacked,pinFirst,filterCount,onCustomize,height,
 chartHeight,hidden,onHide,expanded,onExpand}:OiByStrikeSectionProps){
 const path=underlying?`/api/derivatives/oi-by-strike?underlying=${encodeURIComponent(underlying)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<OiByStrike>(path,seq);
 const body=read.data,state=stateOf(read,'No open interest has been captured for this expiry yet.');
 const [sort,setSort]=useState<SortState>(null);
 const rows=body?.rows||[];
 const subtitle=underlying?`${underlying}${body?.expiry?` · ${body.expiry}`:''}${body?.days_to_expiry!=null?` · ${dteText(body.days_to_expiry)}`:''}`:'';
 const columns:Column<StrikeOi>[]=useMemo(()=>[
  {key:'strike',label:'Strike',width:80,value:VALUE.strike,render:r=><Cell text={strikeText(r.strike)} bold size={12}/>},
  {key:'ce_oi',label:'Call OI',width:74,align:'right',value:VALUE.ce_oi,render:r=><Cell text={compact(r.ce_oi)}/>},
  {key:'ce_chg',label:'Call chg',width:84,align:'right',value:VALUE.ce_chg,
   render:r=><Cell text={signedUnits(r.ce_oi_change_day)} color={tone(r.ce_oi_change_day)}/>},
  {key:'ce_buildup',label:'Call build-up',width:108,value:VALUE.ce_buildup,
   render:r=><Cell text={buildupLabel(r.ce_buildup_day)} color={buildupColor(buildupTone(r.ce_buildup_day))}/>},
  {key:'pe_oi',label:'Put OI',width:74,align:'right',value:VALUE.pe_oi,render:r=><Cell text={compact(r.pe_oi)}/>},
  {key:'pe_chg',label:'Put chg',width:84,align:'right',value:VALUE.pe_chg,
   render:r=><Cell text={signedUnits(r.pe_oi_change_day)} color={tone(r.pe_oi_change_day)}/>},
  {key:'pe_buildup',label:'Put build-up',width:108,grow:true,value:VALUE.pe_buildup,
   render:r=><Cell text={buildupLabel(r.pe_buildup_day)} color={buildupColor(buildupTone(r.pe_buildup_day))}/>},
 ],[]);
 const items=useMemo(():TableItem<StrikeOi>[]=>{
  const list=sort?sortRows(rows,VALUE[sort.key]||(()=>null),sort.dir):rows;
  return list.map(row=>({kind:'row' as const,key:`s-${row.strike}`,row}));
 },[rows,sort]);
 const tableStyle=stacked?{height}:{flex:2,minWidth:0,height};
 const chartStyle=stacked?{height:chartHeight+150}:{flex:1,minWidth:0,height};
 // A widget the reader closed stays closed; a widget expanded anywhere on the page is shown alone.
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const table=!shows('oi_table')?null:<WidgetFrame key="oi_table" name="OI by strike" subtitle={subtitle} body={body}
  state={underlying?state:{phase:'empty',text:'Add a "Symbol is …" filter to see open interest by strike.'}}
  onRefresh={read.reload} filterCount={filterCount} onCustomize={onCustomize}
  onExpand={onExpand?()=>onExpand('oi_table'):undefined} expanded={expanded==='oi_table'}
  onClose={onHide?()=>onHide('oi_table'):undefined} showsSignals={['oi_change_day','buildup_day']}
  note={maxPainBasis(body?.total_ce_oi,body?.total_pe_oi)} style={tableStyle}>
  <Table label="Open interest by strike" columns={columns} items={items} sort={sort} pinFirst={pinFirst}
   onSort={key=>setSort(s=>nextSort(s,key))}
   onRowPress={row=>onTarget({underlying,instrumentToken:null,label:underlying,
    detail:`Strike ${strikeText(row.strike)}`})}
   rowLabel={row=>`Strike ${strikeText(row.strike)}. Call open interest ${compact(row.ce_oi)}, change ${signedUnits(row.ce_oi_change_day)}, ${buildupLabel(row.ce_buildup_day)}. Put open interest ${compact(row.pe_oi)}, change ${signedUnits(row.pe_oi_change_day)}, ${buildupLabel(row.pe_buildup_day)}`}/>
 </WidgetFrame>;
 const chart=!shows('oi_chart')?null:<OiChart key="oi_chart" body={body} state={underlying?state:{phase:'empty',
  text:'Add a "Symbol is …" filter to see open interest by strike.'}} underlying={underlying} subtitle={subtitle}
  onRefresh={read.reload} onExpand={onExpand?()=>onExpand('oi_chart'):undefined} expanded={expanded==='oi_chart'}
  onClose={onHide?()=>onHide('oi_chart'):undefined} height={chartHeight} style={chartStyle}/>;
 if(!table&&!chart)return null;
 return <Section title="OI by strike" subtitle="Where the open interest actually stands at this 15-min reading, and the strike the standing book pays least at." stacked={stacked}>
  {table}{chart}
 </Section>;
}
function OiChart({body,state,underlying,subtitle,onRefresh,onExpand,expanded,onClose,height,style}:{body:OiByStrike|null;
 state:any;underlying:string;subtitle:string;onRefresh:()=>void;onExpand?:()=>void;expanded?:boolean;
 onClose?:()=>void;height:number;style?:any}){
 const [width,setWidth]=useState(0);
 const rows=body?.rows||[],peak=oiPeak(rows);
 const spotIndex=nearestStrikeIndex(rows,body?.spot),painIndex=nearestStrikeIndex(rows,body?.max_pain_strike);
 const inner=Math.max(0,width-16),step=rows.length?inner/rows.length:0,bar=Math.max(2,Math.min(9,step/2-1));
 const plot=height-PAD_BOTTOM;
 const h=(v:number|null)=>v==null||!peak?0:Math.max(v>0?1:0,(v/peak)*plot);
 return <WidgetFrame name={underlying?`${underlying} · call and put open interest by strike`:'OI by strike'}
  subtitle={subtitle} body={body} state={state} onRefresh={onRefresh} onExpand={onExpand} expanded={expanded}
  onClose={onClose} showsSignals={['oi_change_day']} style={style}
  note="Each bar is the open interest standing at this 15-min reading. Max pain is computed from that same standing book (§3.6); neither is a forecast.">
  <ScrollView style={{flex:1}} contentContainerStyle={{padding:10,gap:10}}>
   <View style={[s.row,{gap:12,flexWrap:'wrap'}]}>
    <View style={[s.row,{gap:5}]}><View style={{width:9,height:9,borderRadius:2,backgroundColor:CE_COLOR}}/>
     <T style={{fontSize:11,color:C.muted}}>Call OI</T></View>
    <View style={[s.row,{gap:5}]}><View style={{width:9,height:9,borderRadius:2,backgroundColor:PE_COLOR}}/>
     <T style={{fontSize:11,color:C.muted}}>Put OI</T></View>
    <View style={{flex:1}}/>
    <T style={{fontSize:11,color:C.muted}}>Max pain {body?.max_pain_strike!=null?strikeText(body.max_pain_strike):'—'}{body?.spot!=null?` · spot ${price(body.spot)}`:''}</T>
   </View>
   <View onLayout={e=>{const w=Math.round(e.nativeEvent.layout.width);setWidth(v=>Math.abs(v-w)<3?v:w)}} style={{height}}>
    {width>0&&!!peak&&<Svg width={width} height={height}
     accessibilityLabel={`Open interest by strike for ${underlying}. Largest position ${compact(peak)} contracts.`}>
     {rows.map((row,i)=>{
      const x=8+i*step,ce=h(row.ce_oi),pe=h(row.pe_oi);
      return <React.Fragment key={row.strike}>
       <Rect x={x} y={plot-ce} width={bar} height={ce} fill={CE_COLOR} opacity={0.85} rx={1}/>
       <Rect x={x+bar+1} y={plot-pe} width={bar} height={pe} fill={PE_COLOR} opacity={0.85} rx={1}/>
      </React.Fragment>;
     })}
     <Line x1={0} x2={width} y1={plot} y2={plot} stroke={C.line} strokeWidth={1}/>
     {painIndex!=null&&<>
      <Line x1={8+painIndex*step+bar} x2={8+painIndex*step+bar} y1={0} y2={plot} stroke={C.amber}
       strokeDasharray="3 3" strokeWidth={1}/>
      <SvgText x={Math.min(width-46,8+painIndex*step+bar+3)} y={11} fill={C.amber} fontSize={9}>max pain</SvgText>
     </>}
     {spotIndex!=null&&<>
      <Line x1={8+spotIndex*step+bar} x2={8+spotIndex*step+bar} y1={0} y2={plot} stroke={C.mint} strokeWidth={1}/>
      <SvgText x={Math.min(width-28,8+spotIndex*step+bar+3)} y={plot-4} fill={C.mint} fontSize={9}>spot</SvgText>
     </>}
     {rows.map((row,i)=>(i%Math.max(1,Math.ceil(rows.length/6))===0?
      <SvgText key={`t${row.strike}`} x={8+i*step} y={height-4} fill={C.muted} fontSize={9}>{strikeText(row.strike)}</SvgText>:null))}
    </Svg>}
   </View>
   <T style={{fontSize:10,lineHeight:14,color:C.muted}}>{maxPainBasis(body?.total_ce_oi,body?.total_pe_oi)}</T>
  </ScrollView>
 </WidgetFrame>;
}
