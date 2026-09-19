// The screener: the Derivative tab's workhorse list, and the widget the "Unusual activity" card grew into. One row
// per contract that cleared the liquidity floors AND every filter the server was able to apply, with the §3 signals
// on the row — premium traded, volume against the contract's own median, volume to open interest, both open-interest
// changes and both build-up labels.
//
// THE ONE RULE THIS FILE EXISTS TO HOLD: a filter is rendered as ACTIVE only when the server's own `applied` list
// says it applied it. Not when the browser sent it. Not when it is in the reader's rule list. Not when the parameter
// looks right. That exact mistake cost a day here — a sample-size filter shown as active while it silently deleted
// every row — so the state of every rule comes from logic.filterStatuses and from nowhere else, the header counts
// only the applied ones, the strip above the rows shows each rule's real state, and a filter the server did NOT
// apply is said in plain words in amber under the rows, which is a genuine caveat on the data in front of the reader.
//
// Every honesty rule the card had still holds: the floors are the server's, a volume reading without enough sessions
// behind it is the words "no baseline" (§3.2), the 15-minute and day-on-day build-up labels are never merged (§3.1),
// a number that was not captured is a dash, and nothing here is filtered in the browser.
import React,{useMemo,useState} from 'react';
import {View,Pressable} from 'react-native';
import {C,Icon,T,s} from '../ui';
import type {Read} from './useDerivatives';
import {WidgetFrame,metaText,stateOf,tone,buildupColor,typeColor} from './frame';
import {Table,Cell,type Column,type TableItem} from './Table';
import {BUILDUP_WINDOWS,appliedCount,appliedText,buildupLabel,buildupTone,clock,compact,contractSummary,
 coverageText,crore,dteText,nextSort,notAppliedText,optionTone,price,ratio,rowKey,screenerEmptyDetail,shortDate,
 signed,signedUnits,sortRows,strike as strikeText,units,volumeRatioShort,volumeToOi,volumeToOiHot,
 type FilterStatus,type ReadingChoice,type SortState} from './logic';
import type {ScreenerRow,Screener,ChartTarget} from './types';
/** The captured value behind each column, used by the header sort (and nothing else). */
const VALUE:Record<string,(row:ScreenerRow)=>unknown>={
 time:r=>r.captured_at,symbol:r=>r.underlying,summary:r=>r.tradingsymbol,expiry:r=>r.expiry,
 dte:r=>r.days_to_expiry,strike:r=>r.strike,spot:r=>r.spot,oi:r=>r.oi,oi_chg:r=>r.oi_change_day,
 oi_chg_15m:r=>r.oi_change_15m_pct,volume:r=>r.volume,vol_ratio:r=>r.volume_ratio,vol_oi:r=>r.volume_to_oi,
 premium:r=>r.premium_cr,buildup:r=>r.buildup_day,
};
/** How a rule's state is drawn. ONLY `applied` gets the live treatment; everything else reads as off, because
 *  everything else IS off as far as the rows underneath are concerned. */
const STATE_STYLE:Record<string,{color:string;border:string;suffix:string}>={
 applied:{color:C.green,border:C.green,suffix:''},
 pending:{color:C.muted,border:C.line,suffix:' · not confirmed'},
 not_applied:{color:C.amber,border:'#4A3E1E',suffix:' · NOT applied'},
 unsupported:{color:C.muted,border:C.line,suffix:' · not supported'},
};
/** The strip over the rows: every rule the reader set, each wearing the state the SERVER gave it. */
export function FilterStrip({statuses}:{statuses:FilterStatus[]}){
 if(!statuses.length)return null;
 return <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
  {statuses.map(status=>{
   const style=STATE_STYLE[status.state]||STATE_STYLE.pending;
   return <View key={status.key} accessibilityRole="text"
    accessibilityLabel={`${status.text}. ${status.state==='applied'?'Applied by the server.':status.reason}`}
    style={[s.row,{gap:4,paddingHorizontal:7,paddingVertical:2,borderRadius:6,borderWidth:1,
     borderColor:style.border}]}>
    <Icon name={status.state==='applied'?'check':'alert-circle'} size={10} color={style.color}/>
    <T numberOfLines={1} style={{fontSize:10,lineHeight:14,color:style.color}}>{status.text}{style.suffix}</T>
   </View>;
  })}
 </View>;
}
/** One small segmented control: the choices, which one is on, and what it is for. */
function Segments({label,value,options,onPick}:{label:string;value:string;
 options:{value:string;label:string;detail?:string}[];onPick?:(v:string)=>void}){
 if(!options.length||!onPick)return null;
 return <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
  <T style={metaText}>{label}</T>
  {options.map(option=>{
   const active=option.value===value;
   return <Pressable key={option.value} accessibilityRole="button"
    accessibilityState={{selected:active}}
    accessibilityLabel={`${label}: ${option.label}${option.detail?`, ${option.detail}`:''}`}
    onPress={()=>onPick(option.value)}
    style={(st:any)=>[{minHeight:24,paddingHorizontal:8,justifyContent:'center',borderRadius:6,borderWidth:1,
     borderColor:active?C.green:C.line,
     backgroundColor:active?C.soft:st.hovered||st.focused?C.dark:'transparent'}]}>
    <T numberOfLines={1} style={{fontSize:10,lineHeight:14,color:active?C.green:C.ink}}>{option.label}</T>
   </Pressable>;
  })}
 </View>;
}
/** The strip over the rows: every rule's SERVER-given state, which 15-minute reading these rows are from, and
 *  which build-up window a build-up rule reads. The reading is a control and never a silent redirect — a block
 *  that quietly showed an earlier reading's rows under a newer as-of would be worse than an empty list. */
function ScreenerBar({statuses,readings,at,onAt,buildupWindow,onBuildupWindow}:{statuses:FilterStatus[];
 readings?:ReadingChoice[];at?:string;onAt?:(at:string)=>void;
 buildupWindow?:string;onBuildupWindow?:(w:string)=>void}){
 const [openReadings,setOpenReadings]=useState(false);
 const list=readings||[];
 const current=list.find(r=>r.value===at)||list[0];
 const nothing=!statuses.length&&list.length<2&&!buildupWindow;
 if(nothing)return null;
 return <View style={{gap:6}}>
  {!!statuses.length&&<FilterStrip statuses={statuses}/>}
  {/* §3.1's two windows are never mixed, so the window travels with the rule that reads one */}
  {!!buildupWindow&&<Segments label="Build-up read over" value={buildupWindow}
   options={BUILDUP_WINDOWS} onPick={onBuildupWindow}/>}
  {list.length>1&&<View style={{gap:4}}>
   <Pressable accessibilityRole="button" accessibilityState={{expanded:openReadings}}
    accessibilityLabel={`15-minute reading: ${current?.label||'the newest'}. ${openReadings?'Hide':'Show'} the readings this store holds`}
    onPress={()=>setOpenReadings(o=>!o)}
    style={(st:any)=>[s.row,{gap:5,alignSelf:'flex-start',minHeight:24,paddingHorizontal:8,borderRadius:6,
     borderWidth:1,borderColor:C.line,backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
    <Icon name="clock" size={10} color={C.muted}/>
    <T numberOfLines={1} style={{fontSize:10,lineHeight:14,color:C.muted}}>
     Reading: {current?.label||'the newest'}{current?.detail?` · ${current.detail}`:''}</T>
    <Icon name={openReadings?'chevron-up':'chevron-down'} size={10} color={C.muted}/>
   </Pressable>
   {/* the short label is built in logic.readingChoices, not cut out of the long one here: a transform on
       copy at render time is a place a sentence can be mangled while every check stays green */}
   {openReadings&&<Segments label="Show" value={at||current?.value||''}
    options={list.map(r=>({value:r.value,label:r.short,detail:r.detail}))}
    onPick={v=>{setOpenReadings(false);onAt?.(v)}}/>}
  </View>}
 </View>;
}
export type UnusualWidgetProps={read:Read<Screener>;statuses:FilterStatus[];seq:number;target:ChartTarget|null;
 onTarget:(t:ChartTarget)=>void;onCustomize:()=>void;
 /** The 15-minute readings the store holds, and which one is on screen. Choosing one is the READER's move. */
 readings?:ReadingChoice[];at?:string;onAt?:(at:string)=>void;
 /** Which build-up window a "Build-up is …" rule reads. Empty when no such rule is set. */
 buildupWindow?:string;onBuildupWindow?:(w:string)=>void;
 onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;
 pinFirst?:boolean;style?:any};
export function UnusualWidget({read,statuses,target,onTarget,onCustomize,readings,at,onAt,buildupWindow,
 onBuildupWindow,onExpand,expanded,onClose,pinFirst,style}:UnusualWidgetProps){
 const body=read.data;
 const state=stateOf(read,'No contract clears the liquidity floors and the applied filters at this 15-min reading.');
 const [sort,setSort]=useState<SortState>(null);
 const required=body?.baseline_sessions_required??3;
 const rows=body?.rows||[];
 const pick=(row:ScreenerRow)=>onTarget({underlying:row.underlying,instrumentToken:row.instrument_token,
  label:row.tradingsymbol||row.underlying,
  detail:`${strikeText(row.strike)} ${row.instrument_type} · ${dteText(row.days_to_expiry)}`});
 const columns:Column<ScreenerRow>[]=useMemo(()=>[
  {key:'time',label:'Time',width:54,value:VALUE.time,render:r=><Cell text={clock(r.captured_at)} color={C.muted}/>},
  {key:'symbol',label:'Symbol',width:94,value:VALUE.symbol,filter:onCustomize,
   render:r=><Cell text={r.underlying||'—'} bold size={12}/>},
  {key:'summary',label:'Summary',width:150,grow:true,value:VALUE.summary,filter:onCustomize,
   render:r=><Cell text={contractSummary(r)} color={typeColor(optionTone(r.instrument_type))} bold size={12}/>},
  {key:'expiry',label:'Exp. date',width:102,value:VALUE.expiry,filter:onCustomize,
   render:r=><Cell text={shortDate(r.expiry)||'—'} color={C.muted}/>},
  {key:'dte',label:'DTE',width:60,align:'right',value:VALUE.dte,filter:onCustomize,
   render:r=><Cell text={r.days_to_expiry==null?'—':String(r.days_to_expiry)}/>},
  {key:'strike',label:'Strike',width:72,align:'right',value:VALUE.strike,render:r=><Cell text={strikeText(r.strike)}/>},
  {key:'spot',label:'Spot',width:76,align:'right',value:VALUE.spot,render:r=><Cell text={price(r.spot)} color={C.muted}/>},
  {key:'oi',label:'OI',width:62,align:'right',value:VALUE.oi,render:r=><Cell text={compact(r.oi)}/>},
  {key:'oi_chg',label:'OI chg (day)',width:124,align:'right',value:VALUE.oi_chg,filter:onCustomize,
   render:r=><View style={{alignItems:'flex-end'}}>
    <Cell text={signedUnits(r.oi_change_day)} color={tone(r.oi_change_day)}/>
    <Cell text={signed(r.oi_change_day_pct)} color={C.muted} size={9}/></View>},
  {key:'oi_chg_15m',label:'OI chg (15m)',width:124,align:'right',value:VALUE.oi_chg_15m,filter:onCustomize,
   render:r=><View style={{alignItems:'flex-end'}}>
    <Cell text={signedUnits(r.oi_change_15m)} color={tone(r.oi_change_15m)}/>
    <Cell text={signed(r.oi_change_15m_pct)} color={C.muted} size={9}/></View>},
  {key:'volume',label:'Volume',width:82,align:'right',value:VALUE.volume,
   render:r=><Cell text={compact(r.volume)}/>},
  {key:'vol_ratio',label:'Vol vs median',width:130,align:'right',value:VALUE.vol_ratio,filter:onCustomize,
   render:r=><Cell text={volumeRatioShort(r,required)}
    color={r.volume_baseline==='ok'&&r.volume_ratio!=null?C.ink:C.muted}/>},
  {key:'vol_oi',label:'Vol/OI',width:82,align:'right',value:VALUE.vol_oi,filter:onCustomize,
   render:r=><View style={[s.row,{gap:3}]}>
    {volumeToOiHot(r)&&<Icon name="alert-circle" size={11} color={C.amber}/>}
    <Cell text={ratio(r.volume_to_oi)} color={volumeToOiHot(r)?C.amber:C.ink}/></View>},
  {key:'premium',label:'Premium ₹cr',width:116,align:'right',value:VALUE.premium,filter:onCustomize,
   render:r=><Cell text={crore(r.premium_cr)} bold/>},
  {key:'buildup',label:'Build-up',width:110,value:VALUE.buildup,filter:onCustomize,render:r=><View>
   <Cell text={buildupLabel(r.buildup_day)} color={buildupColor(buildupTone(r.buildup_day))}/>
   <Cell text={`15m ${buildupLabel(r.buildup_15m)}`} color={C.muted} size={9}/></View>},
 ],[onCustomize,required]);
 // Nothing is dropped here: the rows the server returned are the rows on screen, in its order or in one the
 // header sort re-arranged. A sort re-orders that list and never re-queries it, so the as-of line and the
 // floors printed for this block still describe exactly these rows.
 const items=useMemo(():TableItem<ScreenerRow>[]=>{
  const list=sort?sortRows(rows,VALUE[sort.key]||(()=>null),sort.dir):rows;
  return list.map((row,i)=>({kind:'row' as const,key:rowKey(row,i),row}));
 },[sort,rows]);
 const on=appliedCount(statuses);
 const off=notAppliedText(statuses);
 // A count is a claim about an answer, so it is printed only when there IS an answer. While the screener is
 // loading, or unavailable, "0 contracts over the floors" would be a number we do not have — and the whole
 // point of this panel is that it never shows one of those.
 const note=<View style={{gap:2}}>
  {state.phase==='ready'&&<T style={metaText}>
   {rows.length} contract{rows.length===1?'':'s'} over the floors
   {on?`, narrowed by ${on} applied filter${on===1?'':'s'}`:', with no filter applied by the server'}
   {sort?`, sorted by ${columns.find(c=>c.key===sort.key)?.label||sort.key}.`:', largest premium traded first.'}
  </T>}
  {/* The count alone is not the answer. A reading that scanned 27,671 rows across 216 underlyings and passed
      none of them is a very different thing from a reading that was barely taken, and a reader who is shown
      an empty list is owed that difference rather than left to guess at it. */}
  {state.phase==='ready'&&!!coverageText(body)&&<T style={metaText}>{coverageText(body)}</T>}
  {/* the amber on this panel is spent here and nowhere else: a filter the reader set that did NOT narrow these
      rows is a caveat on the data itself, and it is said where the rows are */}
  {!!off&&<T style={[metaText,{color:C.amber}]}>{off}</T>}
 </View>;
 return <WidgetFrame name="Screener" subtitle={appliedText(statuses)} body={body} state={state}
  onRefresh={read.reload}
  filterCount={on} onCustomize={onCustomize} onExpand={onExpand} expanded={expanded} onClose={onClose}
  showsSignals={['premium_cr','volume_ratio','volume_to_oi','buildup_day','buildup_15m','oi_change_day',
   'oi_change_15m','spot']}
  toolbar={<ScreenerBar statuses={statuses} readings={readings} at={at} onAt={onAt}
   buildupWindow={buildupWindow} onBuildupWindow={onBuildupWindow}/>}
  note={note} inBlock style={style}
  emptyDetail={`A contract is listed only when it clears all three liquidity floors, which are named in How to
   read this. ${screenerEmptyDetail(body)}`.replace(/\s+/g,' ').trim()}>
  <Table label="Screener" columns={columns} items={items} sort={sort} pinFirst={pinFirst} rowHeight={38}
   onSort={key=>setSort(s=>nextSort(s,key))} onRowPress={pick}
   selected={row=>row.instrument_token!=null&&target?.instrumentToken===row.instrument_token}
   rowLabel={row=>`${row.tradingsymbol||row.underlying}. ${contractSummary(row)}. ${crore(row.premium_cr)} traded. ${buildupLabel(row.buildup_day)} on the day, ${buildupLabel(row.buildup_15m)} over the last 15 minutes. Volume ${volumeRatioShort(row,required)} of its own median, ${units(row.volume)} traded. Volume to open interest ${volumeToOi(row)}`}/>
 </WidgetFrame>;
}
