// OI by strike: CE against PE at each strike of the chosen expiry, drawn through the tab's ONE template -
// [ Chart ][ Content ]. The bars take the chart panel; the numeric table takes the content panel. The bars are
// the STORED open interest; nothing is smoothed, interpolated or projected, and max pain and the captured spot
// are marked where they actually fall.
//
// The two panels are LINKED, and so is the rest of the tab: pointing at a bar lights that strike's row in the
// table beside it, and lights the same strike on the option chain and in the volatility-by-strike list. It is a
// highlight and nothing else - it selects nothing, it re-queries nothing, and it says nothing the bars do not.
import React,{useMemo,useState} from 'react';
import {View,ScrollView} from 'react-native';
import Svg,{Rect,Line,Text as SvgText} from 'react-native-svg';
import {C,T,s} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {Block,CrosshairReadout,WidgetFrame,metaText,stateOf,tone,buildupColor,useHoverIndex,
 type HeadlineProps,type PaneStyle} from './frame';
import {Table,Cell,type Column,type TableItem} from './Table';
import {DASH,asOfText,buildupLabel,buildupTone,compact,dteText,maxPainAgainstSpot,maxPainBasis,nearestStrikeIndex,
 nextSort,oiPeak,price,signedUnits,sortRows,strike as strikeText,type SortState} from './logic';
import type {OiByStrike,StrikeOi,ChartTarget} from './types';
const CE_COLOR='#F17D87',PE_COLOR='#39E5A3',PAD_BOTTOM=18;
// Kept as one double-quoted constant so the sentence the panel prints is byte-for-byte the one it printed
// before it became conditional. Not one word of it changed.
const OI_CHART_NOTE="Each bar is the open interest standing at this 15-min reading. Max pain is computed from that same standing book (§3.6); neither is a forecast.";
export const OI_LINK_TEXT='Pointing at a bar lights that strike in the table beside it, and wherever else the same strike is on this tab. It highlights; it changes nothing.';
const VALUE:Record<string,(row:StrikeOi)=>unknown>={strike:r=>r.strike,ce_oi:r=>r.ce_oi,ce_chg:r=>r.ce_oi_change_day,
 ce_buildup:r=>r.ce_buildup_day,pe_oi:r=>r.pe_oi,pe_chg:r=>r.pe_oi_change_day,pe_buildup:r=>r.pe_buildup_day};
export type OiByStrikeSectionProps={underlying:string;badge:string;linked?:boolean;expiry:string;
 /** The 15-min reading the TAB is on - the one its screener resolved. It matters: the newest reading of a
  *  session rebuilt from candles carries no spot, and this block marks spot on its bars. */
 at?:string;seq:number;
 onTarget:(t:ChartTarget)=>void;stacked?:boolean;pinFirst?:boolean;filterCount?:number;onCustomize?:()=>void;
 height?:number;hidden?:Record<string,boolean>;onHide?:(key:string)=>void;expanded?:string;
 onExpand?:(key:string)=>void;highlight?:number|null;onHighlight?:(strike:number|null)=>void};
export function OiByStrikeSection({underlying,badge,linked,expiry,at,seq,onTarget,stacked,pinFirst,filterCount,
 onCustomize,height,hidden,onHide,expanded,onExpand,highlight,onHighlight}:OiByStrikeSectionProps){
 const path=underlying?`/api/derivatives/oi-by-strike?underlying=${encodeURIComponent(underlying)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}${at?`&at=${encodeURIComponent(at)}`:''}`:null;
 const read=useDerivativeRead<OiByStrike>(path,seq);
 const body=read.data,state=stateOf(read,'No open interest has been captured for this expiry yet.');
 const [sort,setSort]=useState<SortState>(null);
 const rows=body?.rows||[];
 const subtitle=underlying?underlying+(body?.expiry?' · '+body.expiry:'')+(body?.days_to_expiry!=null?' · '+dteText(body.days_to_expiry):''):'';
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
  return list.map(row=>({kind:'row' as const,key:'s-'+row.strike,row}));
 },[rows,sort]);
 const idle=!underlying;
 const shows=(key:string)=>!hidden?.[key]&&(!expanded||expanded===key);
 const emptyState=underlying?state
  :{phase:'empty' as const,text:'Click any row on the screener above, or choose a symbol in Customize, to see open interest by strike.'};
 // THE FIRST THING TO LOOK AT: the strike the standing book pays least at, against the spot it was captured
 // beside. Never a bare strike, and never a level - this is arithmetic over the open interest standing now.
 const headline:HeadlineProps=underlying
  ?{label:'Max pain strike',value:strikeText(body?.max_pain_strike),
    against:maxPainAgainstSpot(body?.max_pain_strike,body?.spot,
     body?.max_pain_strike!=null&&body?.spot!=null?body.max_pain_strike-body.spot:null),
    reason:'No max-pain strike has been computed for this expiry at this 15-min reading.'}
  :{label:'Max pain strike',value:DASH,
    reason:'Click any row on the screener above, or choose a symbol in Customize, to point this block at one.'};
 const chart=(style:PaneStyle)=>!shows('oi_chart')?null:<OiChart key="oi_chart" body={body} state={emptyState}
  underlying={underlying} subtitle={subtitle} onRefresh={read.reload}
  onExpand={onExpand?()=>onExpand('oi_chart'):undefined} expanded={expanded==='oi_chart'}
  onClose={onHide?()=>onHide('oi_chart'):undefined} highlight={highlight} onHighlight={onHighlight}
  style={style}/>;
 const content=(style:PaneStyle)=>!shows('oi_table')?null:<WidgetFrame key="oi_table" name="OI by strike"
  subtitle={subtitle} body={body} state={emptyState} onRefresh={read.reload} filterCount={filterCount}
  onCustomize={onCustomize} onExpand={onExpand?()=>onExpand('oi_table'):undefined}
  expanded={expanded==='oi_table'} onClose={onHide?()=>onHide('oi_table'):undefined}
  showsSignals={['oi_change_day','buildup_day']} inBlock
  note={idle?undefined:maxPainBasis(body?.total_ce_oi,body?.total_pe_oi)} style={style}>
  <Table label="Open interest by strike" columns={columns} items={items} sort={sort} pinFirst={pinFirst}
   onSort={key=>setSort(s=>nextSort(s,key))}
   onRowHover={row=>onHighlight?.(row?Number(row.strike):null)}
   lit={row=>highlight!=null&&Math.abs(Number(row.strike)-highlight)<0.5}
   onRowPress={row=>onTarget({underlying,instrumentToken:null,label:underlying,
    detail:'Strike '+strikeText(row.strike)})}
   rowLabel={row=>'Strike '+strikeText(row.strike)+'. Call open interest '+compact(row.ce_oi)+', change '+signedUnits(row.ce_oi_change_day)+', '+buildupLabel(row.ce_buildup_day)+'. Put open interest '+compact(row.pe_oi)+', change '+signedUnits(row.pe_oi_change_day)+', '+buildupLabel(row.pe_buildup_day)}/>
 </WidgetFrame>;
 return <Block title="OI by strike"
  subtitle="Where the open interest actually stands at this 15-min reading, and the strike the standing book pays least at."
  asOf={asOfText(body?.as_of)} badge={badge} linked={linked} headline={headline}
  stacked={stacked} height={height} idle={idle} chart={chart} content={content}/>;
}
function OiChart({body,state,underlying,subtitle,onRefresh,onExpand,expanded,onClose,highlight,onHighlight,
 style}:{body:OiByStrike|null;state:any;underlying:string;subtitle:string;onRefresh:()=>void;
 onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;highlight?:number|null;
 onHighlight?:(strike:number|null)=>void;style?:any}){
 const [box,setBox]=useState({w:0,h:0});
 const rows=body?.rows||[],peak=oiPeak(rows);
 const spotIndex=nearestStrikeIndex(rows,body?.spot),painIndex=nearestStrikeIndex(rows,body?.max_pain_strike);
 const height=Math.max(80,box.h);
 const inner=Math.max(0,box.w-16),step=rows.length?inner/rows.length:0,bar=Math.max(2,Math.min(9,step/2-1));
 const plot=height-PAD_BOTTOM;
 const h=(v:number|null)=>v==null||!peak?0:Math.max(v>0?1:0,(v/peak)*plot);
 // Hovering a bar tells the reader which strike it is and what is standing at it - and lights the same strike
 // in the table beside it and on every other panel of the tab that carries it.
 const {cursor,hover}=useHoverIndex((x,width)=>{
  if(!rows.length||step<=0)return null;
  const scale=width>0&&box.w>0?box.w/width:1;
  return Math.max(0,Math.min(rows.length-1,Math.floor((x*scale-8)/step)));
 });
 const hoveredRow=cursor!=null?rows[cursor]:null;
 React.useEffect(()=>{onHighlight?.(hoveredRow?Number(hoveredRow.strike):null)},[hoveredRow,onHighlight]);
 const litIndex=highlight==null?null:nearestStrikeIndex(rows,highlight);
 return <WidgetFrame name={underlying?underlying+' · call and put open interest by strike':'OI by strike'}
  subtitle={subtitle} body={body} state={state} onRefresh={onRefresh} onExpand={onExpand} expanded={expanded}
  onClose={onClose} showsSignals={['oi_change_day']} inBlock style={style}
  note={<View style={{gap:3}}><T style={metaText}>{OI_CHART_NOTE}</T>
   <T style={metaText}>{OI_LINK_TEXT}</T></View>}>
  <View style={{flex:1,minHeight:0,padding:10,gap:8}}>
   {hoveredRow
    ?<CrosshairReadout time={strikeText(hoveredRow.strike)}
      rows={[{label:'Call OI',value:compact(hoveredRow.ce_oi),color:CE_COLOR},
       {label:'Put OI',value:compact(hoveredRow.pe_oi),color:PE_COLOR}]}/>
    :<View style={[s.row,{gap:12,flexWrap:'wrap',minHeight:24}]}>
      <View style={[s.row,{gap:5}]}><View style={{width:9,height:9,borderRadius:2,backgroundColor:CE_COLOR}}/>
       <T style={metaText}>Call OI</T></View>
      <View style={[s.row,{gap:5}]}><View style={{width:9,height:9,borderRadius:2,backgroundColor:PE_COLOR}}/>
       <T style={metaText}>Put OI</T></View>
      <View style={{flex:1}}/>
      <T style={metaText}>Max pain {body?.max_pain_strike!=null?strikeText(body.max_pain_strike):DASH}{body?.spot!=null?' · spot '+price(body.spot):''}</T>
     </View>}
   <View style={{flex:1,minHeight:80}} {...hover}
    onLayout={e=>{const {width,height:hh}=e.nativeEvent.layout;const w=Math.round(width),y=Math.round(hh);
     setBox(v=>Math.abs(v.w-w)<3&&Math.abs(v.h-y)<3?v:{w,h:y})}}>
    {box.w>0&&!!peak&&<Svg width={box.w} height={height}
     accessibilityLabel={'Open interest by strike for '+underlying+'. Largest position '+compact(peak)+' contracts.'}>
     {/* the lit strike, drawn UNDER the bars so it never hides one */}
     {litIndex!=null&&<Rect x={8+litIndex*step-2} y={0} width={bar*2+5} height={plot} fill={C.mint} opacity={0.14}
      rx={2}/>}
     {rows.map((row,i)=>{
      const x=8+i*step,ce=h(row.ce_oi),pe=h(row.pe_oi);
      return <React.Fragment key={row.strike}>
       <Rect x={x} y={plot-ce} width={bar} height={ce} fill={CE_COLOR} opacity={0.85} rx={1}/>
       <Rect x={x+bar+1} y={plot-pe} width={bar} height={pe} fill={PE_COLOR} opacity={0.85} rx={1}/>
      </React.Fragment>;
     })}
     <Line x1={0} x2={box.w} y1={plot} y2={plot} stroke={C.line} strokeWidth={1}/>
     {painIndex!=null&&<>
      <Line x1={8+painIndex*step+bar} x2={8+painIndex*step+bar} y1={0} y2={plot} stroke={C.amber}
       strokeDasharray="3 3" strokeWidth={1}/>
      <SvgText x={Math.min(box.w-46,8+painIndex*step+bar+3)} y={11} fill={C.amber} fontSize={9}>max pain</SvgText>
     </>}
     {spotIndex!=null&&<>
      <Line x1={8+spotIndex*step+bar} x2={8+spotIndex*step+bar} y1={0} y2={plot} stroke={C.mint} strokeWidth={1}/>
      <SvgText x={Math.min(box.w-28,8+spotIndex*step+bar+3)} y={plot-4} fill={C.mint} fontSize={9}>spot</SvgText>
     </>}
     {rows.map((row,i)=>(i%Math.max(1,Math.ceil(rows.length/6))===0?
      <SvgText key={'t'+row.strike} x={8+i*step} y={height-4} fill={C.muted} fontSize={9}>{strikeText(row.strike)}</SvgText>:null))}
    </Svg>}
   </View>
   <T style={metaText}>{maxPainBasis(body?.total_ce_oi,body?.total_pe_oi)}</T>
  </View>
 </WidgetFrame>;
}
