// THE ONE SCREENER ON THIS TAB, PINNED.
//
// It used to appear twice - once as the "Unusual activity" section and again down the left of the delta-OI
// block - each copy eating a quarter of its row, and the owner said so: seven blocks, seven shapes, the same
// list over and over. There is now exactly ONE, it sits at the top of the tab, it STAYS there while the page
// scrolls, and it drives everything under it: clicking a row points every block at that underlying.
//
// It is not one of the blocks. Every block below is [ Chart ][ Content ] on one template; this is the control
// surface those blocks answer to, so it is full width, short enough to stay pinned without eating the page,
// and it is the only thing on the tab that carries the 15-min reading control.
//
// The read is made HERE, once, and handed UP to the page as well as down into the widget: the page needs it to
// resolve the tab's default symbol (the busiest underlying by premium AT THE READING ON SCREEN), and the header
// needs the same envelope the rows came from so its as-of, its floors and its `applied` list can never describe
// a different answer from the rows underneath.
import React,{useMemo,useState} from 'react';
import {View,TextInput} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {LinkText} from '../discover/parts';
import {IconButton} from '../layout';
import type {Read} from './useDerivatives';
import {IDLE_H,InfoDisclosure,SymbolPill,head,type InfoGroup} from './frame';
import {UnusualWidget} from './UnusualWidget';
import {SIGNAL_W} from './SignalTable';
import {SignalPanel} from './SignalPanel';
import {SummaryPanel} from './SummaryPanel';
import {WhatsHappening} from './WhatsHappening';
import {alwaysApplied,appliedCount,asOfText,coverageText,customizeLabel,filterStatuses,floorsDegradedNote,
 floorsText,notAppliedText,readingChoices,readingIsFallback,readingNote,rulesText,
 SEARCH_WHAT,UNUSUAL_COLOUR_TEXT,UNUSUAL_COUNTS_TEXT,UNUSUAL_HONEST_TEXT,UNUSUAL_RULE_TEXT,UNUSUAL_SORT_TEXT,
 UNUSUAL_WHAT,rankingLabel,rankingText,
 type FilterRule,type FilterStatus} from './logic';
import type {ChartTarget,OiGrid,Screener} from './types';

/** What the screener measures, said once, behind the block's one control. */
export const SCREENER_WHAT='One row per contract that cleared the liquidity floors at this 15-min reading, largest premium traded first. Every number on a row was captured; nothing on it is a forecast.';
export const SCREENER_FILTER_TEXT='A filter is shown as active only when the server says it applied it. A filter it did not apply is named under the rows, and the rows are not narrowed by it.';
export const SCREENER_SIGNALS_TEXT='Volume vs median is the contract\'s cumulative volume by this time of day against its own median over the last 10 sessions (§3.2) — under three sessions it reads "no baseline", never a number. Vol/OI is day volume against yesterday\'s closing open interest (§3.3). Premium traded is volume × average price (§3.4).';
export const SCREENER_PICK_TEXT='Clicking a row points EVERY block on this tab at that underlying. The symbol it is showing is on this bar, and on the header of every block below it.';
/** What the default view is, why it is the default, and what may and may not be said about a whole name. */
export const SCREENER_GROUP_WHAT='One row per instrument: every name with a contract over the liquidity floors at this 15-min reading, the unusual first. The row covers that name\u2019s whole F&O book \u2014 calls, puts and futures \u2014 because the question it answers is whether anything is happening in the NAME. It is one row per name because a list of CONTRACTS is a list of whichever name holds the most of them: one busy index can hold a hundred, and then every visible row is that one name.';
export const SCREENER_GROUP_SUMS='Contracts, calls, puts and futures are counts. Premium traded, volume, open interest and the open-interest change are sums over every contract that name has over the floors, options and futures together \u2014 rupees and contracts, which add. The calls/puts split describes the OPTIONS half only: a future is not a call, not a put and not a side of either, so a name with futures and no listed options says so rather than reading as nought and nought.';
export const SCREENER_GROUP_REFUSED='Two \u00a73 signals are NOT aggregated, because a mean of ratios is not a ratio. "Vol vs median \u00b7 highest" is the largest reading among that instrument\u2019s contracts, with the contract it belongs to named beside it; "Vol/OI over 1" is a count of contracts that traded more today than was standing at the previous close. Neither is computed by the store for a FUTURES contract, so both travel with the number of contracts they were counted over, and an instrument whose whole book is futures shows a dash rather than a nought \u2014 nought would say nothing unusual, and the truth is not measured. The \u00a73.1 build-up label describes one contract, so no instrument-level label is shown at all; the counts are in each row\u2019s spoken description.';
export const SCREENER_GROUP_DRILL='Clicking an instrument points every block on this tab at it. Its individual contracts are on the option chain block below.';
/** The pinned bar's own height. Short on purpose: it has to stay on screen while nine blocks scroll past it,
 *  and a driver that eats half the viewport is not a driver. The rows scroll inside it, and the expand control
 *  gives the reader the whole page when they want to browse the list rather than pick from it.
 *
 *  RAISED FROM 320. At 320 the screener showed two rows of 216, the signal table five readings of 25, and
 *  the explanation beside them had 210px of body for about 530px of content — headline visible, ladder and
 *  cross-market rows below the fold. One number, and all three panes gain by it. */
export const RAIL_H=430;
/** The widths the two flexible panes refuse to go below, and the number is arithmetic rather than taste: a
 *  1,366px window — the narrowest common laptop — leaves 1,326px after the page gutters, the signal table
 *  is a fixed 556, and the two gaps take 20, so the other two panes have 750 between them. 350 each is that
 *  with slack for rounding and borders; at 375 it wrapped on the exact tie. Anything wider splits evenly.
 *  Below this the row wraps and the explanation takes a full line of its own rather than all three being
 *  crushed together. */
export const SCREENER_MIN_W=350,SUMMARY_MIN_W=350;

export type ScreenerRailProps={read:Read<Screener>;rules:FilterRule[];rulesLine:string;
 ruleLabel?:(rule:FilterRule)=>string;seq:number;badge:string;linked?:boolean;
 target:ChartTarget|null;onTarget:(t:ChartTarget)=>void;onCustomize:()=>void;
 /** Which 15-min reading is on screen, and the reader's move to any other one. */
 at:string;onAt:(at:string)=>void;buildupWindow:string;onBuildupWindow:(w:string)=>void;
 symbol:string;
 /** The expiry the tab resolved, so the explanation names the same contract month every block is on. */
 expiry?:string;
 /** The TAB's one ΔOI-grid read, shared with the ΔOI block below: the explanation is built on it. */
 gridRead:Read<OiGrid>;
 /** The two whole-book series, read once by the page and handed down. */
 pcr?:{points?:unknown[]}|null;futures?:{points?:unknown[]}|null;
 maxPain?:{points?:unknown[]}|null;iv?:{points?:unknown[]}|null;
 /** Where positions stand at this reading (oi-by-strike), for the pane's first-reading view. */
 standing?:any;
 /** The tab-wide strike highlight, so a strike the explanation names lights up everywhere it appears. */
 highlight?:number|null;onHighlight?:(v:number|null)=>void;
 pinFirst?:boolean;show:boolean;height?:number;
 onExpand?:()=>void;expanded?:boolean};
export function ScreenerRail({read,rules,rulesLine,ruleLabel,seq,badge,linked,target,onTarget,onCustomize,
 at,onAt,buildupWindow,onBuildupWindow,symbol,expiry,gridRead,pcr,futures,maxPain,iv,standing,highlight,onHighlight,
 pinFirst,show,height,onExpand,expanded}:ScreenerRailProps){
 const body=read.data;
 // The ONE place the tab decides whether a filter is in force. A null body - loading, an error, a refusal -
 // leaves every rule pending, which is not active.
 const statuses:FilterStatus[]=useMemo(()=>filterStatuses(rules||[],body,ruleLabel),[rules,body,ruleLabel]);
 const on=appliedCount(statuses);
 const off=notAppliedText(statuses);
 const readings=useMemo(()=>readingChoices(body),[body]);
 const hasBuildupRule=(rules||[]).some(r=>r.column==='buildup');
 // WHICH READING IS ON SCREEN, said plainly whenever it is not the newest the store holds. The tab used to
 // open on the newest one full stop, and on a session rebuilt from candles that reading has no premium on any
 // contract - so nothing cleared the floors, the list was empty, no row could be clicked and the whole tab
 // stayed on its fallback index. The server now opens on the newest reading that HAS rows over the floors.
 const note=readingNote(body);
 const fallback=readingIsFallback(body);
 // WHICH FLOORS THE LIST BELOW WAS GATED ON, on the bar rather than behind a chip. A reading with no traded
 // price average has no premium traded, so the premium floor is not applied there and the rows are kept - and
 // a reader must not have to open anything to learn that the screen was gated on two floors and not three.
 const floorNote=floorsDegradedNote(body);
 // THE ONE SEARCH ON THIS TAB. There was no other, so this is it, and it lives here on the bar that already
 // drives every block rather than in a second field the reader would have to tell apart from this one. It
 // narrows the rows the server returned - it does not re-query - so everything this bar prints above the rows
 // still describes exactly the rows underneath.
 const [search,setSearch]=useState('');
 // WHICH MODE THE LEFT PANE IS IN. The events list is the default front door; the instrument table is a
 // click away with everything it has today.
 const [showTable,setShowTable]=useState(false);
 // SEARCHING IS ASKING FOR THE TABLE. The search field sits in this block's header and always has; with
 // the events list in front of the table, typing into it would filter something the reader cannot see.
 // A search opens the table, and leaving the table clears the search — one control, one visible effect.
 const tableMode=showTable||!!search;
 const setMode=(v:boolean)=>{setShowTable(v);if(!v)setSearch('');};
 if(!show)return null;
 const groups:InfoGroup[]=[
  {heading:'Screener',lines:[SCREENER_WHAT,floorsText(body?.floors,body?.floors_text),SCREENER_SIGNALS_TEXT,
   SCREENER_PICK_TEXT]},
  // WHAT AN UNDERLYING ROW IS, and - just as important - what it deliberately is not.
  {heading:'One row per instrument',lines:[SCREENER_GROUP_WHAT,SCREENER_GROUP_SUMS,
   {text:SCREENER_GROUP_REFUSED,tone:'amber' as const},SCREENER_GROUP_DRILL]},
  // What this reading actually held, which reading it is, and what was on for every query whether the reader
  // asked or not.
  {heading:'This 15-min reading',lines:[coverageText(body),body?.scanned_text||'',
   String(body?.reading_rule||''),note?{text:note,tone:'amber' as const}:null,...alwaysApplied(body)]},
  // WHAT IS UNUSUAL, and why the list opens the way it does. Every threshold behind this is the spec's own:
  // a condition is one of the store's two RULES, so there is no constant here to tune or to argue with.
  {heading:'What is unusual',lines:[UNUSUAL_WHAT,UNUSUAL_COUNTS_TEXT,UNUSUAL_RULE_TEXT,UNUSUAL_COLOUR_TEXT,
   {text:UNUSUAL_HONEST_TEXT,tone:'amber' as const}]},
  // THE ORDER THESE ROWS ARE IN, key by key, as the SERVER served it - never a sort this page assumed.
  {heading:'The order these rows are in',lines:[UNUSUAL_SORT_TEXT,...rankingText(body?.ranking)]},
  {heading:'Finding one name',lines:[SEARCH_WHAT]},
  {heading:'Filters',lines:[SCREENER_FILTER_TEXT,rulesLine,off?{text:off,tone:'amber' as const}:null]},
  {heading:'Moneyness',lines:[body?.moneyness_text||'']},
 ];
 return <View style={{gap:8,backgroundColor:C.bg,paddingBottom:10,borderBottomWidth:1,borderColor:C.line}}>
  <View style={{flexDirection:'row',alignItems:'center',flexWrap:'wrap',gap:10}}>
   <View style={{gap:2,flexShrink:1,minWidth:0}}>
    <View style={[s.row,{gap:8}]}>
     <T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:19,lineHeight:26,
      letterSpacing:-0.3}}>Screener</T>
    </View>
    <T style={{fontSize:12,lineHeight:17,color:C.muted}}>
     One row per instrument — calls, puts and futures together. Every name with a contract over the
     liquidity floors at this 15-min reading, {rankingLabel(body?.ranking)||'the unusual first'}. Click one
     to point every block below at it.</T>
   </View>
   <View style={{flex:1,minWidth:190,flexDirection:'row',flexWrap:'wrap',alignItems:'center',
    justifyContent:'flex-end',gap:10}}>
    {/* The link between this bar and the nine blocks under it, said once and drawn the same way in every
        block header. "Showing" is a fact about the tab, not a claim about the market. */}
    {!!badge&&<View style={[s.row,{gap:6}]}>
     <T style={head}>Showing</T><SymbolPill label={badge} linked={linked} large/></View>}
    {/* the ONE search field on this tab, on the bar that drives every block: type a name, get that name */}
    <View style={[s.row,{gap:5,minWidth:168,paddingHorizontal:8,borderRadius:7,borderWidth:1,
     borderColor:search?C.green:C.line,backgroundColor:C.dark}]}>
     <Icon name="search" size={12} color={search?C.green:C.muted}/>
     <TextInput value={search} onChangeText={setSearch} placeholder="Find an instrument" placeholderTextColor={C.muted}
      accessibilityLabel="Find an instrument in this 15-min reading. Narrows the rows below; it does not re-query"
      style={[s.input,{flex:1,minHeight:28,borderWidth:0,backgroundColor:'transparent',fontSize:12,
       paddingHorizontal:0}]}/>
     {!!search&&<IconButton icon="x" label="Clear the instrument search" tooltip="Clear"
      onPress={()=>setSearch('')} style={{width:20,height:20}}/>}
    </View>
    <View style={[s.row,{gap:5}]}><Icon name="clock" size={12} color={C.muted}/>
     <T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:C.muted}}>
      {asOfText(body?.coverage?.at||body?.as_of)}</T></View>
    <InfoDisclosure title="Screener" groups={groups}/>
    <LinkText label={customizeLabel(on)}
     a11y={'Customize the screener: '+on+' filter'+(on===1?'':'s')+' applied by the server. Open the filter builder'}
     onPress={onCustomize}/>
   </View>
  </View>
  {/* NOT the newest reading. Amber, because the newest state of the book is not what is on screen, which is a
      caveat on the data - and the sentence names the reading, names the newest, and says why. */}
  {!!note&&<View role="note" style={[s.row,{gap:7,paddingHorizontal:10,paddingVertical:6,borderRadius:9,
   borderWidth:1,borderColor:fallback?'#4A3E1E':C.line,backgroundColor:fallback?C.amberBg:C.dark}]}>
   <Icon name={fallback?'alert-triangle':'clock'} size={12} color={fallback?C.amber:C.muted}/>
   <T style={{flex:1,fontSize:11,lineHeight:16,color:fallback?C.amber:C.muted}}>{note}</T>
  </View>}
  {/* A SHORTER FLOOR SET IS A CAVEAT ON THE LIST, so it is amber and it is on the bar. It is never silent:
      the alternative is a reader taking a two-floor list for a three-floor one. */}
  {!!floorNote&&<View role="note" style={[s.row,{gap:7,paddingHorizontal:10,paddingVertical:6,borderRadius:9,
   borderWidth:1,borderColor:'#4A3E1E',backgroundColor:C.amberBg}]}>
   <Icon name="alert-triangle" size={12} color={C.amber}/>
   <T style={{flex:1,fontSize:11,lineHeight:16,color:C.amber}}>{floorNote}</T>
  </View>}
  {/* THE SCREENER, AND THE SIGNAL TABLE BESIDE IT. The owner: "we should show something like this in right
      side of the screener." They are the same height to the pixel, they share this one bar's as-of and this
      one bar's symbol, and neither repeats what the other says. On a narrow window the table drops under the
      screener rather than being squeezed: two dense tables at half width is not crystal clear to anybody. */}
  {/* THREE PANES, and the first two are exactly the two that were here before. The owner, on a version
      that put the explanation where the table had been: "why did u remove what already has". The screener
      and the signal table are untouched — same widths, same heights, same reads — and the explanation is
      ADDED to their right.

      The row WRAPS rather than squeezing. Three dense panes need about 1,400px; below that the
      explanation takes a full-width line of its own under the pair instead of all three being crushed,
      which is the same rule the table already followed on a narrow window. */}
  <View style={{flexDirection:pinFirst?'column':'row',flexWrap:pinFirst?'nowrap':'wrap',gap:10,
   alignItems:'stretch'}}>
   {/* THE FRONT DOOR: what is happening, not what exists. The screener is the other mode of this same pane
       — every column, filter, sort and control it has today, one click behind "All instruments". An EVEN
       split of the free space with the explanation: a table degrades by scrolling sideways, prose degrades
       by becoming unreadable. */}
   <View style={{flex:1,minWidth:pinFirst?undefined:SCREENER_MIN_W}}>
    <WhatsHappening read={read} at={body?.coverage?.at||body?.as_of||at} seq={seq}
     symbol={symbol} onTarget={onTarget}
     showTable={tableMode} onShowTable={setMode}
     onExpand={onExpand} expanded={expanded}
     style={{height:read.phase==='error'?IDLE_H:height||RAIL_H}}
     table={<UnusualWidget read={read} statuses={statuses} seq={seq} target={target} onTarget={onTarget}
      onCustomize={onCustomize} pinFirst={pinFirst}
      readings={readings} at={body?.coverage?.at||body?.as_of||at} onAt={onAt}
      search={search}
      buildupWindow={hasBuildupRule?buildupWindow:''} onBuildupWindow={onBuildupWindow}
      style={{flex:1,borderWidth:0,borderRadius:0}}/>}/>
   </View>
   <SignalPanel symbol={symbol} read={gridRead} pcr={pcr} maxPain={maxPain} iv={iv} standing={standing}
    style={{width:pinFirst?undefined:SIGNAL_W,height:height||RAIL_H}}/>
   {/* THE ADDITION: the same ten contracts the table beside it lists, said in plain language. */}
   <View style={{flex:1,minWidth:pinFirst?undefined:SUMMARY_MIN_W}}>
    <SummaryPanel symbol={symbol} expiry={expiry} read={gridRead}
     pcr={pcr} futures={futures} standing={standing}
     highlight={highlight} onHighlight={onHighlight} onTarget={onTarget}
     style={{height:height||RAIL_H}}/>
   </View>
  </View>
 </View>;
}
