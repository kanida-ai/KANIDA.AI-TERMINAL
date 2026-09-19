// The screener block: the tab's workhorse list on the left and the linked chart of whatever row is picked on the
// right, on the ΔOI block's rhythm — ONE as-of line, ONE "How to read this", one symbol badge and one Customize
// link in the Section header, and panels of a fixed height under it.
//
// The read is made HERE, once, because the header needs the same envelope the rows came from: its as-of is the
// block's one as-of, its floors are the block's one set of floors, and its `applied`/`available` lists are what
// decides whether a filter is shown as active at all. Reading it in two places would let the header and the rows
// describe two different answers to the same question.
import React,{useMemo,useState} from 'react';
import {LinkText} from '../discover/parts';
import {useDerivativeRead} from './useDerivatives';
import {IDLE_H,InfoDisclosure,Section,type InfoGroup} from './frame';
import {UnusualWidget} from './UnusualWidget';
import {oneSentence} from './SessionBlocks';
import {BUILDUP_WINDOWS,BUILDUP_WINDOW_DEFAULT,SCREENER_PATH,alwaysApplied,appliedCount,asOfText,coverageText,
 customizeLabel,filterStatuses,floorsText,notAppliedText,readingChoices,screenerEmptyDetail,screenerQuery,
 type FilterRule,type FilterStatus} from './logic';
import type {ChartTarget,Screener} from './types';

/** What the screener measures, said once, behind the block's one control. */
export const SCREENER_WHAT='One row per contract that cleared the liquidity floors at this 15-min reading, largest premium traded first. Every number on a row was captured; nothing on it is a forecast.';
export const SCREENER_FILTER_TEXT='A filter is shown as active only when the server says it applied it. A filter it did not apply is named under the rows, and the rows are not narrowed by it.';
export const SCREENER_SIGNALS_TEXT='Volume vs median is the contract\'s cumulative volume by this time of day against its own median over the last 10 sessions (§3.2) — under three sessions it reads "no baseline", never a number. Vol/OI is day volume against yesterday\'s closing open interest (§3.3). Premium traded is volume × average price (§3.4).';
export const SCREENER_PICK_TEXT='Clicking a row points every block on this tab at that underlying.';

export type ScreenerSectionProps={title:string;rules:FilterRule[];rulesLine:string;
 ruleLabel?:(rule:FilterRule)=>string;seq:number;badge:string;
 target:ChartTarget|null;onTarget:(t:ChartTarget)=>void;onCustomize:()=>void;
 stacked?:boolean;pinFirst?:boolean;height:number;
 /** The chart tile beside the table, built by the page so the whole tab shares one chart read. It is a
  *  function of the collapse decision, because the two panels of a block must shrink TOGETHER: a section
  *  where one panel shrank and the other did not reads worse than the hole it was meant to close. */
 chart?:(idle:boolean)=>React.ReactNode;showTable:boolean;
 onExpand?:()=>void;expanded?:boolean;onClose?:()=>void;style?:any};
export function ScreenerSection({title,rules,rulesLine,ruleLabel,seq,badge,target,onTarget,onCustomize,stacked,
 pinFirst,height,chart,showTable,onExpand,expanded,onClose,style}:ScreenerSectionProps){
 // Which 15-minute reading the block is on. Omitted, the server reads its newest — and the reader is NEVER
 // moved off it silently. The newest reading of a session can hold far less than an earlier one (a reading
 // rebuilt from candles carries no VWAP, so no premium, so nothing clears the §3 floors), and an empty list
 // that quietly showed older rows under a newer as-of would be the worst of both. So the reading is a control:
 // the block says what this one held, lists the ones the store has, and moves only when the reader says so.
 const [at,setAt]=useState('');
 // Which build-up window a "Build-up is …" rule reads. §3.1's two labels are never mixed in one answer, so the
 // window travels with the rule rather than being assumed.
 const [window,setWindow]=useState(BUILDUP_WINDOW_DEFAULT);
 const query=useMemo(()=>screenerQuery(rules||[],{buildupWindow:window,at}),[rules,window,at]);
 const read=useDerivativeRead<Screener>(`${SCREENER_PATH}${query}`,seq);
 const body=read.data;
 // The ONE place the tab decides whether a filter is in force. A null body — loading, an error, a refusal —
 // leaves every rule pending, which is not active.
 const statuses:FilterStatus[]=useMemo(()=>filterStatuses(rules||[],body,ruleLabel),[rules,body,ruleLabel]);
 const on=appliedCount(statuses);
 const off=notAppliedText(statuses);
 const readings=useMemo(()=>readingChoices(body),[body]);
 const hasBuildupRule=(rules||[]).some(r=>r.column==='buildup');
 // A block showing one sentence takes the height of one sentence, not the height of a table (frame.IDLE_H).
 // The screener route not being there yet is exactly that case; a reading that returned no rows is not, and
 // keeps its full height, because rows will be there at the next one.
 const idle=oneSentence(read.phase);
 const tall=idle?IDLE_H:height;
 const table=showTable?<UnusualWidget key="unusual" read={read} statuses={statuses} seq={seq} target={target}
  onTarget={onTarget} onCustomize={onCustomize} pinFirst={pinFirst}
  readings={readings} at={body?.coverage?.at||body?.as_of||''} onAt={setAt}
  buildupWindow={hasBuildupRule?window:''} onBuildupWindow={setWindow}
  onExpand={onExpand} expanded={expanded} onClose={onClose}
  style={stacked?{height:tall}:{flex:2,minWidth:0,height:tall}}/>:null;
 const tile=chart?chart(idle):null;
 if(!table&&!tile)return null;
 const groups:InfoGroup[]=[
  {heading:title,lines:[SCREENER_WHAT,floorsText(body?.floors,body?.floors_text),SCREENER_SIGNALS_TEXT,
   SCREENER_PICK_TEXT]},
  // What this reading actually held, and what was on for every query whether the reader asked or not.
  {heading:'This 15-min reading',lines:[coverageText(body),body?.scanned_text||'',...alwaysApplied(body)]},
  {heading:'Filters',lines:[SCREENER_FILTER_TEXT,rulesLine,
   off?{text:off,tone:'amber' as const}:null]},
  {heading:'Moneyness',lines:[body?.moneyness_text||'']},
 ];
 return <Section title={title}
  subtitle="Every contract over the liquidity floors, with the §3 signals on the row."
  asOf={asOfText(body?.coverage?.at||body?.as_of)} badge={badge}
  info={<InfoDisclosure title={title} groups={groups}/>}
  actions={<LinkText label={customizeLabel(on)}
   a11y={`Customize the screener: ${on} filter${on===1?'':'s'} applied by the server. Open the filter builder`}
   onPress={onCustomize}/>}
  stacked={stacked}>{table}{tile}</Section>;
}
