// BLOCK 1, LEFT PANE — what is happening, rather than what exists.
//
// The screener answers "which names have a contract over the floors": 216 of them, and at the 15:45 reading
// of 18 Sep every row a reader could see said "Nothing flagged". That is filing, not intelligence. This pane
// answers the question a trader actually opens the tab with — IS ANYTHING HAPPENING, AND WHERE.
//
// THE WHOLE BOOK, AND ONE REQUEST. It reads `/api/derivatives/events`, where the walk runs ONCE per 15-min
// reading over every instrument the capture covered and is handed to every reader after it. The first
// version of this pane did the walk in the browser, one grid request per instrument, and so could only
// afford eight of them; the honest note under it had to say the other two hundred were never read. They are
// all read now, on the server, in about eight hundred milliseconds cold and nothing at all after that.
//
// THE SCREENER IS NOT DELETED. It is the other mode of this pane, one click away, with every column,
// filter, sort and control it has today. Nothing was taken out of it.
import React,{useMemo} from 'react';
import {View,Pressable,ScrollView} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {useDerivativeRead,type Read} from './useDerivatives';
import {WidgetFrame,head,metaText,stateOf} from './frame';
import {clock,strike as strikeText} from './logic';
import type {ChartTarget,MarketEvent,MarketEvents,Screener} from './types';

export const EVENTS_EMPTY='No instrument could be read at this 15-min reading.';

export type WhatsHappeningProps={
 /** The screener read the rail already makes — used for the table mode and for the instrument count. */
 read:Read<Screener>;
 at:string;seq:number;
 /** The instrument the tab is pointed at, and the one control that moves it — the same one a screener row
  *  calls, so picking here and picking there are the same act. */
 symbol:string;onTarget:(t:ChartTarget)=>void;
 /** The screener, rendered by the rail, shown instead of this list when the reader asks for it. */
 table:React.ReactNode;showTable:boolean;onShowTable:(v:boolean)=>void;
 style?:any;onExpand?:()=>void;expanded?:boolean};

export function WhatsHappening({read,at,seq,symbol,onTarget,table,showTable,onShowTable,
 style,onExpand,expanded}:WhatsHappeningProps){
 // ONE request for the whole book at this reading. `at` is the tab's own boundary, so this list and every
 // block below it describe the same moment.
 const path=`/api/derivatives/events${at?`?at=${encodeURIComponent(at)}`:''}`;
 const events=useDerivativeRead<MarketEvents>(showTable?null:path,seq);
 const body=events.data;
 const rows=(body?.rows||[]) as MarketEvent[];
 const state=stateOf(events,EVENTS_EMPTY);
 const screener=read.data;
 const total=body?.instruments??((screener?.rows||[]).length);
 const live=body?.measured??0;

 // The ones with something to say are already first: the server orders by that, then by the size behind
 // each row. Nothing is re-sorted here, so the list a reader sees is the list the server can explain.
 const list=useMemo(()=>rows,[rows]);

 return <WidgetFrame name={showTable?'All instruments':"What's happening"}
  subtitle={showTable
   ?`${total} instruments over the liquidity floors at this 15-min reading`
   :`${live} of ${total} instruments are reading a behaviour`}
  body={showTable?screener:body} state={showTable?undefined:state}
  onRefresh={showTable?read.reload:events.reload}
  onExpand={onExpand} expanded={expanded} inBlock
  emptyDetail="Each instrument needs two 15-min readings of its at-the-money contracts before anything can be compared."
  toolbar={<View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
   <Pressable accessibilityRole="button" accessibilityState={{selected:!showTable}}
    accessibilityLabel="Show what is happening across every instrument at this reading"
    onPress={()=>onShowTable(false)}
    style={(st:any)=>[{minHeight:24,paddingHorizontal:9,justifyContent:'center',borderRadius:6,borderWidth:1,
     borderColor:!showTable?C.green:C.line,
     backgroundColor:!showTable||st.hovered?C.soft:'transparent'}]}>
    <T style={{fontSize:10,lineHeight:14,color:!showTable?C.green:C.muted}}>What's happening</T></Pressable>
   <Pressable accessibilityRole="button" accessibilityState={{selected:showTable}}
    accessibilityLabel="Show the full instrument table, with every column, filter and sort"
    onPress={()=>onShowTable(true)}
    style={(st:any)=>[{minHeight:24,paddingHorizontal:9,justifyContent:'center',borderRadius:6,borderWidth:1,
     borderColor:showTable?C.green:C.line,
     backgroundColor:showTable||st.hovered?C.soft:'transparent'}]}>
    <T style={{fontSize:10,lineHeight:14,color:showTable?C.green:C.muted}}>All instruments</T></Pressable>
  </View>}
  note={showTable?undefined
   :`Every instrument the capture covered at this reading, read under rule ${body?.rule_version||''}. `
    +'A row says what its numbers did between two readings; it does not name who traded.'}
  style={style}>
  {showTable
   ? <View style={{flex:1,minHeight:0}}>{table}</View>
   : <ScrollView style={{flex:1}} contentContainerStyle={{paddingVertical:4,paddingHorizontal:8,gap:2}}>
    {list.map(row=>{
     const on=row.underlying===symbol;
     const bits:string[]=[];
     if(row.first_at)bits.push(`since ${clock(row.first_at)}`);
     if(row.lead!=null)bits.push(`${strikeText(row.lead)} ${row.side} largest`);
     // A ROW THAT COULD NOT BE READ SAYS WHICH ABSENCE IT IS. "Nothing happening" and "nothing measurable"
     // are different answers and the list must never blur them.
     const detail=bits.join(' · ')||(row.measured?'':'no comparable value at this reading');
     return <Pressable key={row.underlying} accessibilityRole="button"
      accessibilityState={{selected:on}}
      accessibilityLabel={`${row.underlying}. ${row.headline}.${detail?` ${detail}.`:''} `
       +'Point every block on this tab at this instrument'}
      onPress={()=>onTarget({underlying:row.underlying,instrumentToken:null,label:row.underlying,
       detail:row.headline})}
      style={(st:any)=>[{gap:1,paddingVertical:7,paddingHorizontal:8,borderRadius:8,borderLeftWidth:2,
       borderLeftColor:on?C.green:'transparent',
       backgroundColor:on||st.hovered||st.focused?C.soft:'transparent'}]}>
      <View style={[s.row,{gap:6}]}>
       <View style={{width:6,height:6,borderRadius:3,backgroundColor:row.live?C.green:C.line}}/>
       <T numberOfLines={1} style={{flex:1,fontSize:12,lineHeight:16,fontFamily:'InterSemi',
        color:on?C.green:C.ink}}>{row.underlying}</T>
      </View>
      <T numberOfLines={2} style={{fontSize:11,lineHeight:15,color:row.live?C.ink:C.muted,
       paddingLeft:12}}>{row.headline}</T>
      {!!detail&&<T numberOfLines={1} style={{...metaText,paddingLeft:12,
       fontVariant:['tabular-nums'] as any}}>{detail}</T>}
     </Pressable>;
    })}
    {!!list.length&&<Pressable accessibilityRole="button"
     accessibilityLabel={`Show the full instrument table — all ${total} instruments, with every column`}
     onPress={()=>onShowTable(true)}
     style={(st:any)=>[s.row,{gap:6,marginTop:4,paddingVertical:7,paddingHorizontal:8,borderRadius:8,
      borderTopWidth:1,borderColor:C.line,
      backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
     <Icon name="list" size={12} color={C.muted}/>
     <T style={{flex:1,fontSize:11,lineHeight:15,color:C.muted}}>
      Open the full table — every column, filter and sort</T>
    </Pressable>}
   </ScrollView>}
 </WidgetFrame>;
}
