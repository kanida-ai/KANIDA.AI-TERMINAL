// THE SIGNAL TABLE, to the right of the pinned screener.
//
// The owner specified this column for column: TIME | CALL ACTIVITY | PUT ACTIVITY | OI INTERPRETATION |
// MARKET SIGNAL, one row per 15-min reading, newest first. He asked for it, it was removed at his request, and
// he has now asked for it back with a full specification. It is built as he wrote it.
//
// WHAT THIS FILE MAY AND MAY NOT SAY. Every sentence on it is PRESENT TENSE and about POSITIONING: where open
// interest sits at this reading, and who appears to have put it there. There is no price target on it, no
// support or resistance LEVEL with a number, no buy or sell instruction, and no forecast. The one caveat -
// that this is the conventional reading of option positioning rather than a prediction, and that every opened
// contract has a buyer and a seller - is stated ONCE, behind the panel's "How to read this", and is not
// repeated on every row.
//
// WHERE THE NUMBERS COME FROM. The very ten contracts the ΔOI grid draws: the five calls at and above the
// money and the five puts at and below it, and no others. Nothing here queries a strike the grid is not
// showing, so the table and the tiles can never disagree about the same reading, and the subtitle says which
// ten. The words are FLOW_LABELS - the owner's own source table, already under every tile and already served
// verbatim by derivatives.py - and not one of them is restated here.
//
// AND WHAT IT REFUSES. A reading the store never took is not a row. A reading that WAS taken but carried no
// value on either side is a row that says so, and it carries no signal: dropping it would hide a hole, and
// filling it would be an invention. A side with nothing to compare against an hour back has no direction, and
// the row says "no baseline" rather than guessing one.
import React,{useMemo} from 'react';
import {View,ScrollView} from 'react-native';
import {C,T,s} from '../ui';
import type {Read} from './useDerivatives';
import {InfoDisclosure,WidgetFrame,head,metaText,stateOf,type InfoGroup} from './frame';
import {DASH,GRID_LEGEND_TEXT,SIGNAL_AGGREGATE_TEXT,SIGNAL_BASKET_TEXT,SIGNAL_CAVEAT_TEXT,SIGNAL_NO_BASELINE,
 SIGNAL_RULE_TEXT,SIGNAL_RULE_VERSION,SIGNAL_STRENGTH_TEXT,SIGNAL_WINDOW_MINUTES,asOfText,clock,signalRows,
 type SignalRow,type SignalSide} from './logic';
import type {OiGrid} from './types';

/** The panel's own height, so it stands exactly as tall as the pinned screener beside it. */
export const SIGNAL_W=556;
export const SIGNAL_EMPTY='No 15-min reading of these ten contracts has been captured yet.';
export const SIGNAL_NO_SYMBOL='Click a row on the screener to read the ten at-the-money contracts of that instrument.';
/** Green for the bullish reading, red for the bearish one, neutral for flat — and NEVER on its own: the dot,
 *  the word and the two activity cells that produced it are all on the same row. */
const SIGNAL_TONE:Record<string,string>={strong_bullish:C.green,bullish:C.green,flat:C.muted,
 bearish:C.red,strong_bearish:C.red};
const COLS={time:48,side:100,read:150};

/** One side's cell: the measured strength over the arrows, and the owner's own vocabulary under it. */
function SideCell({side}:{side:SignalSide|null}){
 if(!side)return <View style={{width:COLS.side}}>
  <T style={{fontSize:9,lineHeight:13,color:C.muted}}>{SIGNAL_NO_BASELINE}</T></View>;
 const colour=side.oi_direction==='building'?C.green:side.oi_direction==='unwinding'?C.red:C.muted;
 return <View style={{width:COLS.side,gap:1}}>
  <T numberOfLines={1} style={{fontSize:10,lineHeight:14,fontFamily:'InterSemi',color:colour}}>{side.text}</T>
  <T numberOfLines={2} style={{fontSize:9,lineHeight:12,color:C.muted}}>{side.what_label}</T>
 </View>;
}

export type SignalTableProps={symbol:string;
 /** The TAB's one ΔOI-grid read, made in index.tsx and shared with the ΔOI block. This panel used to make
  *  its own — two fetches of one payload, and two answers that could drift apart at a refresh. */
 read:Read<OiGrid>;style?:any;
 onExpand?:()=>void;expanded?:boolean};
export function SignalTable({symbol,read,style,onExpand,expanded}:SignalTableProps){
 // The SAME read the ΔOI block is built on, so the ten contracts here ARE the ten contracts there.
 const body=read.data;
 const state=symbol?stateOf(read,body?.not_enough_marks||SIGNAL_EMPTY)
  :{phase:'empty' as const,text:SIGNAL_NO_SYMBOL};
 const rows=useMemo(()=>signalRows(body?.rows),[body]);
 const groups:InfoGroup[]=[
  // THE ONE CAVEAT, ONCE. Amber, because it is a caveat on what the table is - not on any single row.
  {heading:'What this table is',lines:[{text:SIGNAL_CAVEAT_TEXT,tone:'amber' as const},SIGNAL_RULE_TEXT]},
  {heading:'Which contracts',lines:[SIGNAL_AGGREGATE_TEXT,SIGNAL_BASKET_TEXT]},
  {heading:'How strong is strong',lines:[SIGNAL_STRENGTH_TEXT]},
  {heading:'The vocabulary',lines:[body?.flow_text||'',GRID_LEGEND_TEXT]},
 ];
 return <WidgetFrame name="Signal by 15-min reading"
  subtitle={symbol?`${symbol} · the same ten at-the-money contracts the ΔOI tiles draw`:''}
  body={body} state={state} onRefresh={read.reload} onExpand={onExpand} expanded={expanded} inBlock
  toolbar={<View style={[s.row,{justifyContent:'flex-end'}]}><InfoDisclosure title="Signal by 15-min reading" groups={groups}/></View>}
  emptyDetail="Each row needs two 15-min readings of these contracts before either side has anything to compare against."
  style={style}>
  <View style={{flex:1,minHeight:0}}>
   <View style={[s.row,{gap:6,paddingHorizontal:10,paddingVertical:4,borderBottomWidth:1,borderColor:C.line}]}>
    <T style={[head,{width:COLS.time}]}>Time</T>
    <T style={[head,{width:COLS.side}]}>Call activity</T>
    <T style={[head,{width:COLS.side}]}>Put activity</T>
    <T style={[head,{width:COLS.read,flex:1}]}>OI interpretation</T>
    <T style={[head,{width:96}]}>Market signal</T>
   </View>
   <ScrollView style={{flex:1}} contentContainerStyle={{paddingBottom:6}}>
    {/* the INDEX leads the key: two readings of a rebuilt session can carry the same stamp, and a row is its
        position in this list either way */}
    {rows.map((row:SignalRow,i)=><View key={`${i}-${row.at||''}`}
     accessibilityRole="text"
     // THE WINDOW IS ON THE ROW. A word that came from a comparison has to say which comparison, and the time
     // column is 48px wide - so it is said here, where a screen reader and the DOM both reach it.
     accessibilityLabel={`${clock(row.at)}. ${row.reason
      ?row.reason
      :`Compared with the ${clock(row.from)} reading, ${row.window_minutes} minutes back. `
       +`Calls ${row.calls?.text||SIGNAL_NO_BASELINE}, ${row.calls?.what_label||''}. `
       +`Puts ${row.puts?.text||SIGNAL_NO_BASELINE}, ${row.puts?.what_label||''}. `
       +`${row.interpretation}. ${row.label}.`}`}
     style={[s.row,{gap:6,paddingHorizontal:10,paddingVertical:5,borderBottomWidth:1,borderColor:'#0F1B22',
      alignItems:'flex-start'}]}>
     <T style={{width:COLS.time,fontSize:10,lineHeight:14,color:C.muted,
      fontVariant:['tabular-nums']}}>{clock(row.at)}</T>
     {/* A READING WITH NO DATA IS NEVER GIVEN A SIGNAL. It keeps its row and says what it has, because a
         reading that was taken and carried nothing is a fact, and closing the gap up would hide it. */}
     {row.reason&&!row.calls&&!row.puts
      ?<T style={{flex:1,fontSize:9,lineHeight:13,color:C.muted}}>{row.reason}</T>
      :<>
       <SideCell side={row.calls}/>
       <SideCell side={row.puts}/>
       <T numberOfLines={2} style={{width:COLS.read,flex:1,fontSize:9,lineHeight:13,
        color:C.muted}}>{row.interpretation||DASH}</T>
       {/* the dot NEVER alone: the owner's own word is beside it on every row */}
       <View style={[s.row,{width:96,gap:4}]}>
        <T style={{fontSize:10,lineHeight:14}}>{row.dot}</T>
        <T numberOfLines={1} style={{flex:1,fontSize:10,lineHeight:14,fontFamily:'InterSemi',
         color:SIGNAL_TONE[row.signal]||C.muted}}>{row.label}</T>
       </View>
      </>}
    </View>)}
   </ScrollView>
   <View style={{paddingHorizontal:10,paddingVertical:4,borderTopWidth:1,borderColor:C.line}}>
    {/* the rule that produced these rows, named on the panel rather than only in the code */}
    <T style={metaText}>{rows.length} 15-min reading{rows.length===1?'':'s'} of these ten contracts, each read
     {' '}against the reading {SIGNAL_WINDOW_MINUTES} minutes before it. Rule {SIGNAL_RULE_VERSION}.
     {' '}{asOfText(body?.as_of)}.</T>
   </View>
  </View>
 </WidgetFrame>;
}
