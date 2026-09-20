// The screener: the Derivative tab's workhorse list, pinned at the top of the page, and the thing every block
// below it answers to.
//
// ONE ROW PER INSTRUMENT. The owner, in his own words: "Just show one row per instrument - like nifty show one
// row, stock show one row."
//
// It used to be a list of CONTRACTS, largest premium traded first. At the 11:30 reading of 18 Sep 2026, 85
// underlyings had a contract over the liquidity floors and NIFTY alone had 104 of the 491 contracts - so a
// hundred-row list was a hundred rows of NIFTY, and the reader's conclusion was the obvious one: no stock is
// active. That is what he meant by "why other stocks are not populating".
//
// There is no view control and no drill-down. One list. Clicking a row points the whole tab at that instrument,
// and the instrument's individual contracts are where they already were - on the option chain block below.
//
// The contract columns are still here, and they are NOT a mode: they are what gets rendered if a server ever
// answers with contract rows, because the table must draw what the server SAYS it served rather than what this
// file hoped for. The reader is never offered the choice.
//
// THE ONE RULE THIS FILE EXISTS TO HOLD: a filter is rendered as ACTIVE only when the server's own `applied`
// list says it applied it. Not when the browser sent it. Not when it is in the reader's rule list. Not when the
// parameter looks right. That exact mistake cost a day here - a sample-size filter shown as active while it
// silently deleted every row - so the state of every rule comes from logic.filterStatuses and from nowhere
// else, the header counts only the applied ones, and a filter the server did NOT apply is said in plain words
// in amber under the rows.
//
// THE SECOND RULE, which the per-underlying view added: NO AGGREGATE IS INVENTED HERE. Every figure on an
// underlying row is a sum of things that add or a count of contracts, computed by the server over every row
// that cleared the floors. The two §3 ratios are not averaged - a mean of ratios is not a ratio - so one
// arrives as the largest reading among that name's contracts with the contract named, and the other as a
// count. The build-up label is a statement about one contract, so there is no underlying-level label at all.
import React,{useMemo,useState} from 'react';
import {View,Pressable} from 'react-native';
import {C,Icon,Sheet,T,s} from '../ui';
import type {Read} from './useDerivatives';
import {WidgetFrame,metaText,head,stateOf,tone,buildupColor,typeColor} from './frame';
import {DASH} from './logic';
import {Table,Cell,type Column,type TableItem} from './Table';
import {BUILDUP_WINDOWS,appliedCount,appliedText,buildupLabel,buildupTone,clock,compact,
 contractSummary,coverageText,crore,dteText,groupContracts,groupHot,groupHotText,groupOptionsNote,
 groupRowLabel,groupSplit,
 groupTopText,groupVolumeRatioMax,groupVolumeRatioText,groupsCountText,nextSort,notAppliedText,optionTone,
 price,ratio,rowKey,screenerEmptyDetail,shortDate,signed,signedUnits,sortRows,strike as strikeText,units,
 volumeRatioShort,volumeToOi,volumeToOiHot,
 searchRows,searchNote,unusualDegree,unusualEvidence,unusualEvidenceSummary,unusualMeasured,unusualRuleBadge,
 unusualRuleDetail,unusualRules,unusualShort,unusualTriggerText,
 UNUSUAL_EVIDENCE_LABEL,UNUSUAL_EVIDENCE_NONE,UNUSUAL_NOT_MEASURED_TEXT,UNUSUAL_RULE_MAX,UNUSUAL_SORT_TEXT,
 type FilterStatus,type ReadingChoice,type SortState} from './logic';
import type {ScreenerRow,ScreenerGroup,Screener,ChartTarget} from './types';
/** The captured value behind each column, used by the header sort (and nothing else). */
const VALUE:Record<string,(row:ScreenerRow)=>unknown>={
 time:r=>r.captured_at,symbol:r=>r.underlying,summary:r=>r.tradingsymbol,expiry:r=>r.expiry,
 dte:r=>r.days_to_expiry,strike:r=>r.strike,spot:r=>r.spot,oi:r=>r.oi,oi_chg:r=>r.oi_change_day,
 oi_chg_15m:r=>r.oi_change_15m_pct,volume:r=>r.volume,vol_ratio:r=>r.volume_ratio,vol_oi:r=>r.volume_to_oi,
 premium:r=>r.premium_cr,buildup:r=>r.buildup_day,
};
/** The same, for an underlying row. Every one of these is a SUM or a COUNT the server computed. */
const GROUP_VALUE:Record<string,(row:ScreenerGroup)=>unknown>={
 symbol:r=>r.underlying,contracts:r=>r.contracts,spot:r=>r.spot,premium:r=>r.premium_cr,volume:r=>r.volume,
 oi:r=>r.oi,oi_chg:r=>r.oi_change_day,vol_ratio:r=>r.volume_ratio_max,hot:r=>r.volume_to_oi_over_1,
 dte:r=>r.days_to_expiry,top:r=>r.top?.premium_cr,
 // The DEGREE is what this column sorts on - a count of distinct §3 conditions, not a score.
 unusual:r=>unusualDegree(r),
};
/** One tone per degree, and the degree is a COUNT of conditions. The tone is never the only carrier: the word
 *  and both counts sit beside it on the row, for a reader who cannot use colour and for one who does not yet
 *  know the convention. */
/** The Unusual column's own width. Everything it draws is clamped to it, so it can never paint across the six
 *  columns to its right the way the unclamped reason line did. It is wide enough for the counts and ONE BADGE
 *  PER RULE beside the evidence control - and there are two rules, so that is the widest it ever has to be. */
export const UNUSUAL_W=250;
/** One tone per CONDITION COUNT, and there are two conditions. There is no third step, because there is no
 *  third rule: the old three-step scale was reading a clamped count of reason SENTENCES. */
const UNUSUAL_TONE:Record<number,string>={1:C.muted,2:C.amber};
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
 *  which build-up window a build-up rule reads. There is no view control here and there must not be: the list
 *  is one row per instrument, full stop. The reading is a control and never a silent redirect - a bar that
 *  quietly showed an earlier reading's rows under a newer as-of would be worse than an empty list. */
function ScreenerBar({statuses,readings,at,onAt,buildupWindow,onBuildupWindow}:{
 statuses:FilterStatus[];
 readings?:ReadingChoice[];at?:string;onAt?:(at:string)=>void;
 buildupWindow?:string;onBuildupWindow?:(w:string)=>void}){
 const [openReadings,setOpenReadings]=useState(false);
 const list=readings||[];
 const current=list.find(r=>r.value===at)||list[0];
 if(!statuses.length&&list.length<2&&!buildupWindow)return null;
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
 /** What the reader typed into the ONE search field on the bar above. It narrows the rows the server already
  *  returned and never re-queries, exactly as the header sort does. */
 search?:string;
 pinFirst?:boolean;style?:any};
/** THE EVIDENCE BEHIND THE BADGE: the rules this instrument's contracts tripped, and the contracts
 *  themselves. It exists because the cell can no longer carry them — and it must not: the concatenated
 *  version ran to 5,015 characters in one table cell for NIFTY, invisible behind a two-line clamp and read
 *  out in full by a screen reader. Nothing here is computed; every value is the server's own. */
function UnusualEvidence({group,onClose}:{group:ScreenerGroup|null;onClose:()=>void}){
 const rules=group?unusualRules(group):[];
 const contracts=group?unusualEvidence(group):[];
 return <Sheet wide visible={!!group} onClose={onClose}
  title={group?`${group.underlying} · why it is flagged`:'Evidence'}
  subtitle={group?unusualEvidenceSummary(group):undefined}>
  <View style={{gap:8}}>
   <T style={head}>The conditions</T>
   {rules.length?rules.map(rule=><View key={rule.rule_id} style={{gap:2,paddingVertical:4,borderTopWidth:1,
    borderColor:C.line}}>
    <T style={{fontSize:13,lineHeight:19,color:C.ink}}>{unusualRuleBadge(rule)}</T>
    {!!unusualRuleDetail(rule)&&
     <T style={{fontSize:11,lineHeight:17,color:C.muted}}>{unusualRuleDetail(rule)}</T>}
    {!!rule.text&&<T style={{fontSize:11,lineHeight:17,color:C.muted}}>{rule.text}</T>}
   </View>):<T style={{fontSize:12,color:C.muted}}>The store flagged contracts here and named no condition.</T>}
  </View>
  <View style={{gap:8}}>
   <T style={head}>The contracts</T>
   {contracts.length?contracts.map(row=><View key={row.tradingsymbol} style={{gap:2,paddingVertical:4,
    borderTopWidth:1,borderColor:C.line}}>
    <T style={{fontSize:12,lineHeight:18,color:C.ink}}>
     {row.tradingsymbol} · {strikeText(row.strike)} {row.instrument_type} · {crore(row.premium_cr)} traded</T>
    {(row.triggers||[]).map((trigger,i)=><T key={i}
     style={{fontSize:11,lineHeight:17,color:C.muted}}>{unusualTriggerText(trigger,rules)}</T>)}
   </View>)
   /* A response that carried the rule totals and not the contracts says exactly that, rather than
      showing an empty list that would read as "no contracts". */
   :<T style={{fontSize:12,lineHeight:18,color:C.muted}}>{UNUSUAL_EVIDENCE_NONE}</T>}
  </View>
 </Sheet>;
}
export function UnusualWidget({read,statuses,target,onTarget,onCustomize,readings,at,
 onAt,buildupWindow,onBuildupWindow,onExpand,expanded,onClose,search,pinFirst,style}:UnusualWidgetProps){
 const body=read.data;
 // WHAT THE SERVER SAYS IT SERVED, not what this file hoped for. One row per instrument is the only list the
 // reader is ever offered; this is here so the table draws contract rows correctly if a server ever answers
 // with them, rather than drawing an instrument's columns over a contract and filling them with blanks.
 const grouped=(body?.view||'underlying')==='underlying';
 const state=stateOf(read,grouped
  ?'No underlying clears the liquidity floors and the applied filters at this 15-min reading.'
  :'No contract clears the liquidity floors and the applied filters at this 15-min reading.');
 const [sort,setSort]=useState<SortState>(null);
 // Which instrument's evidence is open. One drawer for the whole list, never one per row.
 const [evidence,setEvidence]=useState<ScreenerGroup|null>(null);
 const required=body?.baseline_sessions_required??3;
 const served=body?.rows||[];
 // GOING STRAIGHT TO ONE NAME. The reader typed it into the one search field on the bar above; this narrows
 // the rows the SERVER returned and re-queries nothing, so the as-of, the floors and the applied filters
 // printed on this panel still describe exactly the rows underneath.
 const rows=useMemo(()=>searchRows(served as any[],search),[served,search]);
 const pick=(row:ScreenerRow)=>onTarget({underlying:row.underlying,instrumentToken:row.instrument_token,
  label:row.tradingsymbol||row.underlying,
  detail:`${strikeText(row.strike)} ${row.instrument_type} · ${dteText(row.days_to_expiry)}`});
 // Clicking an instrument points the WHOLE TAB at it - the chain, the strikes, the tiles, every session
 // block. Its individual contracts are not opened here: they are on the option chain block below, which is
 // where they already were.
 const pickGroup=(row:ScreenerGroup)=>onTarget({underlying:row.underlying,instrumentToken:null,
  label:row.underlying,
  detail:`${groupContracts(row)} over the floors · ${crore(row.premium_cr)} traded`});
 const contractColumns:Column<ScreenerRow>[]=useMemo(()=>[
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
 // THE MARKET VIEW. Every column is a sum the server computed or a count of contracts. The two that cannot be
 // summed say what they are in their own header, so a reader can never mistake a maximum for an average.
 // THE COLUMNS ANSWER ONE QUESTION: is something happening in this name? Each is a count of contracts or a sum
 // of things that add, except the two §3 signals that cannot be summed - and those say in their own headers
 // exactly what they are, so a maximum can never be read as an average.
 const groupColumns:Column<ScreenerGroup>[]=useMemo(()=>[
  {key:'symbol',label:'Instrument',width:132,value:GROUP_VALUE.symbol,filter:onCustomize,
   render:r=><View style={[s.row,{gap:5}]}>
    {/* the mark's own colour rail, beside a name that is never marked by colour alone: the word and both
        counts are in the column to the right, on the same row */}
    <View style={{width:3,alignSelf:'stretch',borderRadius:2,
     backgroundColor:unusualDegree(r)?UNUSUAL_TONE[unusualDegree(r)]:'transparent'}}/>
    <Cell text={r.underlying||'—'} bold size={12}/>
    {r.underlying_kind==='index'&&<View style={{paddingHorizontal:4,borderRadius:3,borderWidth:1,
     borderColor:C.line}}><T style={{fontSize:8,lineHeight:12,color:C.muted}}>INDEX</T></View>}
   </View>},
  // WHAT IS UNUSUAL IN THIS NAME, AND WHY, in a column of its OWN and on its own two lines.
  //
  // It used to print the store's reason SENTENCES, concatenated. A sentence carries its own multiple, so
  // NIFTY's 80 flagged contracts wrote 124 different ones and this cell held 5,015 characters — clamped to two
  // lines on screen, and read out in full, every character of it, by a screen reader. The count over them said
  // "3 conditions", which was neither a count of condition types nor three of anything.
  //
  // So the cell carries the COUNTS and a BADGE PER RULE, which is short by construction: there are two rules.
  // The contracts behind them are one click away in the evidence drawer, where each one carries its value, the
  // threshold it was compared against, the baseline it was measured against and how many observations that
  // baseline stands on. Nothing is hidden; it is simply not all read out at once.
  //
  // And the two silences are DIFFERENT sentences. "Nothing flagged" is a measured quiet book. "Not measured"
  // is a name the store computes neither §3 ratio over - every futures-only name, 129 of the 214 here - and
  // calling that "nothing flagged" would claim a quiet book where there is simply no measurement.
  {key:'unusual',label:'Unusual',width:UNUSUAL_W,value:GROUP_VALUE.unusual,
   render:r=>{
    const degree=unusualDegree(r);
    const measured=unusualMeasured(r);
    const rules=degree?unusualRules(r):[];
    return <View style={{width:UNUSUAL_W,minWidth:0,gap:2}}>
     <View style={[s.row,{gap:4,minWidth:0}]}>
      {!!degree&&<Icon name="alert-circle" size={11} color={UNUSUAL_TONE[degree]}/>}
      <View style={{flex:1,minWidth:0}}>
       <Cell text={unusualShort(r)} size={degree?11:10} bold={!!degree}
        color={degree?UNUSUAL_TONE[degree]:C.muted}/></View>
     </View>
     {degree
      /* ONE BADGE PER RULE, never one per sentence. Two rules exist, so this line cannot grow. */
      ?<View style={[s.row,{gap:4,minWidth:0,flexWrap:'nowrap'}]}>
        {rules.length
         ?rules.slice(0,UNUSUAL_RULE_MAX).map(rule=><View key={rule.rule_id}
           style={{flexShrink:1,minWidth:0,paddingHorizontal:4,borderRadius:3,borderWidth:1,
            borderColor:C.line}}>
           <T numberOfLines={1} style={{fontSize:8,lineHeight:12,color:C.muted}}>
            {unusualRuleBadge(rule,true)}</T></View>)
         :<Cell text="the store named no condition" color={C.muted} size={9}/>}
        <Pressable accessibilityRole="button"
         accessibilityLabel={`${UNUSUAL_EVIDENCE_LABEL} for ${r.underlying}: ${unusualEvidenceSummary(r)}`}
         onPress={()=>setEvidence(r)}
         style={(st:any)=>[{paddingHorizontal:4,borderRadius:3,borderWidth:1,
          borderColor:st.hovered||st.focused?C.green:C.line,
          backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
         <T style={{fontSize:8,lineHeight:12,color:C.green}}>{UNUSUAL_EVIDENCE_LABEL}</T></Pressable>
       </View>
      :<View style={{width:UNUSUAL_W,minWidth:0}}>
        <Cell text={measured?'':'§3.2 and §3.3 are not computed for futures'}
         color={C.muted} size={9}/></View>}
    </View>;
   }},
  // The whole F&O book of the name, and what it is made of. The calls/puts split is the OPTIONS half; a
  // futures-only name says so rather than reading as "0C / 0P", which would claim its options were quiet.
  {key:'contracts',label:'Contracts',width:132,align:'right',value:GROUP_VALUE.contracts,
   render:r=><View style={{alignItems:'flex-end'}}>
    <Cell text={String(r.contracts)} bold/>
    <Cell text={groupSplit(r)||DASH} color={C.muted} size={9}/></View>},
  {key:'premium',label:'Premium ₹cr',width:126,align:'right',value:GROUP_VALUE.premium,filter:onCustomize,
   render:r=><Cell text={crore(r.premium_cr,(r.premium_cr||0)>=100?0:1)} bold size={12}/>},
  {key:'oi_chg',label:'OI chg (day)',width:126,align:'right',value:GROUP_VALUE.oi_chg,
   render:r=><Cell text={signedUnits(r.oi_change_day)} color={tone(r.oi_change_day)}/>},
  // A COUNT over the tab's own existing threshold - not a mean of ratios.
  // A COUNT over the tab's own existing threshold - not a mean of ratios - and it travels with what it was
  // counted over, because §3.3 is not computed for a futures contract at all.
  {key:'hot',label:'Vol/OI over 1',width:124,align:'right',value:GROUP_VALUE.hot,
   render:r=><View style={{alignItems:'flex-end'}}>
    <View style={[s.row,{gap:3}]}>
     {!!r.volume_to_oi_over_1&&<Icon name="alert-circle" size={11} color={C.amber}/>}
     <Cell text={groupHot(r)} color={r.volume_to_oi_over_1?C.amber:C.muted}/></View>
    <Cell text={groupOptionsNote(r)} color={C.muted} size={9}/></View>},
  // NOT an average. The header says so, the line under it names whose reading it is, and the row's spoken
  // description says how many of this instrument's contracts had a baseline at all.
  {key:'vol_ratio',label:'Vol vs median · highest',width:168,align:'right',value:GROUP_VALUE.vol_ratio,
   render:r=><View style={{alignItems:'flex-end'}}>
    <Cell text={groupVolumeRatioMax(r)} color={r.volume_ratio_max==null?C.muted:C.ink}/>
    <Cell text={r.volume_ratio_max_symbol||(r.options?`${r.volume_baseline_contracts} with a baseline`:'no options listed')}
     color={C.muted} size={9}/></View>},
  {key:'spot',label:'Spot',width:96,align:'right',value:GROUP_VALUE.spot,
   render:r=><Cell text={price(r.spot)} color={C.muted}/>},
  {key:'dte',label:'Front expiry',width:120,value:GROUP_VALUE.dte,filter:onCustomize,
   render:r=><View>
    <Cell text={shortDate(r.expiries?.[0])||'—'}/>
    <Cell text={r.days_to_expiry==null?'—':dteText(r.days_to_expiry)} color={C.muted} size={9}/></View>},
  {key:'top',label:'Busiest contract',width:186,grow:true,value:GROUP_VALUE.top,
   render:r=><View>
    <Cell text={r.top?.tradingsymbol||'—'} color={typeColor(optionTone(r.top?.instrument_type))} size={11}/>
    <Cell text={r.top?crore(r.top.premium_cr):'—'} color={C.muted} size={9}/></View>},
 ],[onCustomize,setEvidence]);
 // Nothing is dropped here: the rows the server returned are the rows on screen, in its order or in one the
 // header sort re-arranged. A sort re-orders that list and never re-queries it, so the as-of line and the
 // floors printed for this block still describe exactly these rows.
 const items=useMemo(():TableItem<any>[]=>{
  const value=grouped?GROUP_VALUE:VALUE;
  const list=sort?sortRows(rows as any[],(value as any)[sort.key]||(()=>null),sort.dir):(rows as any[]);
  return list.map((row,i)=>({kind:'row' as const,
   key:grouped?`g-${(row as ScreenerGroup).underlying||i}`:rowKey(row as ScreenerRow,i),row}));
 },[sort,rows,grouped]);
 const on=appliedCount(statuses);
 const off=notAppliedText(statuses);
 // A count is a claim about an answer, so it is printed only when there IS an answer. While the screener is
 // loading, or unavailable, "0 contracts over the floors" would be a number we do not have.
 const note=<View style={{gap:2}}>
  {state.phase==='ready'&&(grouped
   ?<T style={metaText}>{groupsCountText(body)}
     {on?` Narrowed by ${on} applied filter${on===1?'':'s'}.`:' No filter applied by the server.'}
     {sort?` Sorted by ${groupColumns.find(c=>c.key===sort.key)?.label||sort.key}.`
      /* THE ORDER THE SERVER SAYS IT SERVED, in the server's own words. This line used to describe a sort
         nobody had checked against the one in force. */
      :` ${body?.ranking?.text||UNUSUAL_SORT_TEXT}`}</T>
   :<T style={metaText}>
     {rows.length} contract{rows.length===1?'':'s'} over the floors
     {on?`, narrowed by ${on} applied filter${on===1?'':'s'}`:', with no filter applied by the server'}
     {sort?`, sorted by ${contractColumns.find(c=>c.key===sort.key)?.label||sort.key}.`
      :`. ${body?.ranking?.text||'Largest premium traded first.'}`}
    </T>)}
  {/* The count alone is not the answer. A reading that scanned 27,671 rows across 216 underlyings and passed
      none of them is a very different thing from a reading that was barely taken. */}
  {state.phase==='ready'&&!!coverageText(body)&&<T style={metaText}>{coverageText(body)}</T>}
  {/* what the ONE search field narrowed to, said where the rows are. A name that did not clear the floors at
      this reading is not in the list to be found, and the sentence says so rather than leaving a blank list. */}
  {!!searchNote(search,rows.length,served.length)&&
   <T style={metaText}>{searchNote(search,rows.length,served.length)}</T>}
  {/* the amber on this panel is spent here and nowhere else: a filter the reader set that did NOT narrow these
      rows is a caveat on the data itself, and it is said where the rows are */}
  {!!off&&<T style={[metaText,{color:C.amber}]}>{off}</T>}
 </View>;
 return <><WidgetFrame name="Screener" subtitle={appliedText(statuses)} body={body} state={state}
  onRefresh={read.reload}
  filterCount={on} onCustomize={onCustomize} onExpand={onExpand} expanded={expanded} onClose={onClose}
  showsSignals={['premium_cr','volume_ratio','volume_to_oi','buildup_day','buildup_15m','oi_change_day',
   'oi_change_15m','spot']}
  toolbar={<ScreenerBar statuses={statuses} readings={readings} at={at} onAt={onAt}
   buildupWindow={buildupWindow} onBuildupWindow={onBuildupWindow}/>}
  note={note} inBlock style={style}
  emptyDetail={`A contract is listed only when it clears all three liquidity floors, which are named in How to
   read this. ${screenerEmptyDetail(body)}`.replace(/\s+/g,' ').trim()}>
  {grouped
   ?<Table label="Underlyings over the liquidity floors" columns={groupColumns} items={items} sort={sort}
     pinFirst={pinFirst} rowHeight={40} onSort={key=>setSort(s=>nextSort(s,key))}
     onRowPress={pickGroup}
     selected={row=>!!row.underlying&&target?.underlying===row.underlying}
     rowLabel={row=>groupRowLabel(row,required)}/>
   :<Table label="Screener" columns={contractColumns} items={items} sort={sort} pinFirst={pinFirst}
     rowHeight={38} onSort={key=>setSort(s=>nextSort(s,key))} onRowPress={pick}
     selected={row=>row.instrument_token!=null&&target?.instrumentToken===row.instrument_token}
     rowLabel={row=>`${row.tradingsymbol||row.underlying}. ${contractSummary(row)}. ${crore(row.premium_cr)} traded. ${buildupLabel(row.buildup_day)} on the day, ${buildupLabel(row.buildup_15m)} over the last 15 minutes. Volume ${volumeRatioShort(row,required)} of its own median, ${units(row.volume)} traded. Volume to open interest ${volumeToOi(row)}`}/>}
 </WidgetFrame>
 {/* ONE drawer for the whole list, mounted beside the table rather than inside a row. */}
 <UnusualEvidence group={evidence} onClose={()=>setEvidence(null)}/></>;
}
