// The futures chart in the owner's ΔOI block: one tall panel between the unusual-derivative screener and the
// 2 × 5 grid of ΔOI tiles, as tall as BOTH tile rows together, drawing the front futures contract of the
// block's symbol as candles.
//
// The symbol is NOT resolved here. The block resolves it once (logic.resolveBlockSymbol) and hands the same
// answer to this panel and to all ten tiles, so a screener click re-points the whole block together and the
// chart can never end up on a different underlying from the grid beside it.
//
// What is on screen, and the rules it works to:
//   * 15-minute candles on arrival; a control at the top switches to daily. An interval the server says has
//     no data is DISABLED and its reason is printed UNDER the chart — never hidden, so the reader is told
//     rather than left wondering where the choice went. The control lives in the frame's toolbar slot,
//     outside the state switch, so choosing an empty interval never removes the way back.
//   * The panel is titled with the CONTRACT; its expiry sits in the subtitle beside it, because a title and
//     an expiry crammed into one narrow line is a title cut short. Nothing is abbreviated to make it fit.
//   * The chart is the point of this panel, so it starts directly under the control. What was a stack of
//     paragraphs above it is now one line under it — the last close and how much of the series is drawn —
//     one amber line when something is actually wrong, and the definitions in the block's one info panel.
//   * A candle is drawn only where one was stored. A stored candle missing any of its four prices keeps its
//     slot and is left blank: nothing is interpolated and nothing is carried forward.
//   * Every honest state has its own words: no symbol yet, the endpoint not available yet, no candles for
//     this contract, too few candles to read as a series, and the server's own short-history sentence.
//
// §5 holds here as everywhere else on this tab: the panel describes what was stored. There is no trend call,
// no level, no lean, and no sentence about what comes next. Candle colour is the ordinary close-against-open
// colour code and says nothing beyond those two stored numbers.
import React,{useEffect,useState} from 'react';
import {View,Pressable} from 'react-native';
import Svg,{Line,Rect,Text as SvgText} from 'react-native-svg';
import {C,Icon,T,s} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {WidgetFrame,bodyText,metaText,stateOf,type InfoLine} from './frame';
import {DASH,FUTURES_DEFAULT_INTERVAL,FUTURES_DISPLAY_TEXT,FUTURES_GAP_TEXT,FUTURES_NO_CANDLES,FUTURES_NO_SYMBOL,
 candleAxis,candleAxisLabel,candleAxisTimes,candleDrawnText,candleWindow,candleWindowShort,candleWindowText,
 futuresBars,futuresChartName,futuresChartSpoken,futuresChartSubtitle,futuresChartTitle,
 futuresFewText,futuresIntervalChoices,futuresIntervalNote,futuresNote,futuresShortText,price,priceTick,
 scaleCandles,type FuturesInterval} from './logic';
import type {FuturesChart} from './types';

/** Room for the rupee labels down the left, and for the time labels under the candles. */
const AXIS_W=46,TIME_H=14,PAD=10,MIN_PLOT_H=120;
/** An up candle is closed above its own open, a down candle below it. Nothing more is claimed by the colour. */
const UP=C.green,DOWN=C.red,FLAT=C.muted;
/** The thin line through a candle is its high-to-low range; the box is open-to-close. */
const WICK_W=1,BODY_MIN_H=1;
// Joins the panel's definition lines into one string so the effect that hands them up fires on a real change
// and not on every render. A control character, so it can never appear inside a sentence.
const SPLIT='\u0000';

export type FuturesChartPanelProps={underlying:string;seq:number;onExpand?:()=>void;expanded?:boolean;
 onClose?:()=>void;
 // The panel's own definitions, handed UP to the block's single "How to read this" panel rather than
 // printed over the chart. This is how src/workspace/ChartCentre.tsx moves its drawing notes off the canvas.
 onInfo?:(lines:InfoLine[])=>void;
 style?:any};
export function FuturesChartPanel({underlying,seq,onExpand,expanded,onClose,onInfo,style}:FuturesChartPanelProps){
 const [box,setBox]=useState({w:0,h:0});
 // 15-min is what the block is captured at, so it is what opens; the control below switches to daily.
 const [interval,setInterval]=useState<FuturesInterval>(FUTURES_DEFAULT_INTERVAL);
 const path=underlying
  ?`/api/derivatives/futures-chart?underlying=${encodeURIComponent(underlying)}&interval=${interval}`:null;
 const read=useDerivativeRead<FuturesChart>(path,seq);
 const body=read.data;
 const state=underlying?stateOf(read,FUTURES_NO_CANDLES)
  :{phase:'empty' as const,text:FUTURES_NO_SYMBOL};
 const series=body?.candles||[];
 const plotW=Math.max(0,box.w-AXIS_W),plotH=Math.max(0,box.h-TIME_H);
 // a tail of the series, never a sample of it: 364 bars in a 250px panel is half a pixel each, so the panel
 // draws the most recent ones that are actually legible and says how many of how many those are
 // not named `window`: this renders on web, where that name is taken
 const win=candleWindow(series,plotW);
 const candles=win.candles;
 const windowText=candleWindowText(win);
 const scaled=scaleCandles(candles,plotW,plotH);
 // the same fact in its short form, for the one line under the chart
 const countText=candleWindowShort(win)||candleDrawnText(scaled?.drawn,futuresBars(body));
 // the axis is given the width it is drawn in, so two labels can never be stacked on the same pixels
 const axis=candleAxis(scaled,plotH),times=candleAxisTimes(candles,interval,5,plotW);
 const last=[...candles].reverse().find(c=>typeof c?.close==='number');
 // the server's own sentence when the series is short, and ours when the panel drew almost nothing at all
 const short=futuresShortText(body);
 const few=futuresFewText(scaled?.drawn);
 const choices=futuresIntervalChoices(body);
 const dead=futuresIntervalNote(choices);
 // the expiry rides in the subtitle, so the contract in the title is never cut short
 const subtitle=futuresChartSubtitle(body);
 // what the panel DESCRIBES goes to the block's one info panel; what is wrong with the data stays on screen
 const info=[futuresNote(body,interval),windowText,FUTURES_GAP_TEXT,FUTURES_DISPLAY_TEXT].filter(Boolean).join(SPLIT);
 useEffect(()=>{onInfo?.(info?info.split(SPLIT).map(text=>({text})):[])},[info,onInfo]);
 // the control stays on screen whatever the state is, so an empty interval is never a dead end
 const toolbar=<>
  <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
   {choices.map(choice=>{
    const on=choice.key===interval;
    return <Pressable key={choice.key} accessibilityRole="button"
     accessibilityState={{selected:on,disabled:!choice.available}} disabled={!choice.available}
     accessibilityLabel={choice.available?`Show ${choice.label}`:`${choice.label}, unavailable. ${choice.reason}`}
     onPress={()=>setInterval(choice.key)}
     style={(st:any)=>[{minHeight:28,paddingHorizontal:10,justifyContent:'center',borderRadius:8,borderWidth:1,
      borderColor:on?C.green:C.line,opacity:choice.available?1:0.5,
      backgroundColor:on?C.soft:st.hovered||st.focused?C.dark:'transparent'}]}>
     <T numberOfLines={1} style={{fontSize:11,lineHeight:15,fontFamily:'InterSemi',
      color:on?C.green:choice.available?C.ink:C.muted}}>{choice.label}</T>
    </Pressable>;
   })}
  </View>
  {/* a dead interval is disabled here; its reason is SAID under the chart, never dropped from the control */}
 </>;
 // ONE line under the chart instead of a stack of paragraphs over it: the last close on the left, how much of
 // the series is drawn on the right.
 //
 // Under it, two different things that were being said in the same shout. The reason an interval is DISABLED
 // explains a control the reader can see is off - it is not a failure and it is not about the candles on
 // screen, so it reads as quiet small print behind an info mark. A series the store is genuinely short of IS
 // a caveat on what is drawn, and that keeps the amber. Neither sentence changed a word.
 const thin=[short,few].filter(Boolean).join(' ');
 const note=<View style={{gap:3}}>
  <View style={[s.row,{gap:10}]}>
   <T numberOfLines={1} style={[bodyText,{fontVariant:['tabular-nums']}]}>Last close {price(last?.close)}</T>
   <View style={{flex:1}}/>
   {/* how much of the series is on screen: the window when it is one, the drawn count when it is all of it */}
   {!!countText&&<T numberOfLines={1} style={[metaText,{fontVariant:['tabular-nums']}]}>{countText}</T>}
  </View>
  {!!dead&&<View style={[s.row,{gap:5,alignItems:'flex-start'}]}>
   <View style={{paddingTop:2}}><Icon name="info" size={10} color={C.muted}/></View>
   <T style={[metaText,{flex:1}]}>{dead}</T></View>}
  {!!thin&&<T style={[metaText,{color:C.amber}]}>{thin}</T>}
 </View>;
 return <WidgetFrame name={futuresChartTitle(body?.contract,underlying)} subtitle={subtitle} body={body}
  state={state} onRefresh={read.reload} onExpand={onExpand}
  expanded={expanded} onClose={onClose} toolbar={toolbar} note={note} inBlock style={style}
  emptyDetail="The panel draws only candles that were stored for this contract; it never fills a gap in the series.">
  <View style={{flex:1,minHeight:0,padding:PAD,gap:6}}>
   <View style={{flex:1,minHeight:MIN_PLOT_H}}
    onLayout={e=>{const {width,height}=e.nativeEvent.layout;const w=Math.round(width),h=Math.round(height);
     setBox(v=>Math.abs(v.w-w)<3&&Math.abs(v.h-h)<3?v:{w,h})}}>
    {plotW>0&&plotH>0&&<Svg width={box.w} height={box.h}
     accessibilityLabel={`${futuresChartSpoken({contract:body?.contract,candles},interval,underlying)}${windowText?` ${windowText}`:''}`}>
     {axis.map(t=><SvgTick key={`y${t.v}`} y={t.y} label={priceTick(t.v)} width={box.w} plotH={plotH}/>)}
     {(scaled?.candles||[]).map((candle,i)=>{
      // a candle that was not stored whole keeps its slot and is drawn as nothing at all
      if(!candle)return null;
      const colour=candle.direction==='up'?UP:candle.direction==='down'?DOWN:FLAT;
      const top=Math.min(candle.open,candle.close);
      const height=Math.max(BODY_MIN_H,Math.abs(candle.close-candle.open));
      const x=AXIS_W+candle.x;
      return <React.Fragment key={`c${i}`}>
       <Line x1={x} x2={x} y1={candle.high} y2={candle.low} stroke={colour} strokeWidth={WICK_W}/>
       <Rect x={x-(scaled!.body/2)} y={top} width={scaled!.body} height={height} fill={colour}/>
      </React.Fragment>;
     })}
     {times.map(t=><SvgTime key={`x${t.i}`} label={t.label} x={AXIS_W+(scaled?.step||0)*(t.i+0.5)}
      width={box.w} y={box.h-3}/>)}
    </Svg>}
   </View>
   {/* The first and last candle drawn are already labelled ON the axis (candleAxisTimes always picks the
       first and the last), so a second row repeating them under the plot was chrome saying nothing new. */}
  </View>
 </WidgetFrame>;
}
// The two label helpers are split out only so the chart body above stays readable; neither adds a number.
function SvgTick({y,label,width,plotH}:{y:number;label:string;width:number;plotH:number}){
 if(label===DASH)return null;
 return <>
  <Line x1={AXIS_W} x2={width} y1={Math.min(plotH,Math.max(0,y))} y2={Math.min(plotH,Math.max(0,y))}
   stroke={C.line} strokeWidth={1} strokeDasharray="2 4"/>
  <SvgLabel x={0} y={Math.min(plotH-1,Math.max(8,y+3))} label={label}/>
 </>;
}
function SvgTime({label,x,width,y}:{label:string;x:number;width:number;y:number}){
 if(label===DASH)return null;
 return <SvgLabel x={Math.min(width-32,Math.max(AXIS_W,x-16))} y={y} label={label}/>;
}
function SvgLabel({x,y,label}:{x:number;y:number;label:string}){
 return <SvgText x={x} y={y} fill={C.muted} fontSize={9}>{label}</SvgText>;
}
