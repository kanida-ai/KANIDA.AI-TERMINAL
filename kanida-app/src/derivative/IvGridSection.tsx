// The implied-volatility grid: the SAME ten contracts the ΔOI block draws, in the SAME order, laid out the same
// way, so a reader can put the two blocks side by side and compare them tile for tile.
//
//   row 1 — calls: ATM, ATM+1, ATM+2, ATM+3, ATM+4
//   row 2 — puts:  ATM, ATM−1, ATM−2, ATM−3, ATM−4
//
// The ten slots come from the ΔOI block's own endpoint (`/api/derivatives/oi-grid`), which is the one place the
// at-the-money ladder is resolved on this tab. Taking the ladder from there rather than rebuilding it here is
// what GUARANTEES the two grids are about the same ten contracts: one rule, one answer, two pictures of it.
// Each tile then reads its own contract's volatility from `/api/derivatives/iv-series?...&strike=&option_type=`.
//
// Everything that made the old strike list trustworthy is on every tile, not footnoted once:
//   * COMPUTED, on EVERY tile. Implied volatility is what a pricing model returned when it was asked which
//     volatility reproduces the traded price. Every other number on this tab is one the exchange reported, and a
//     reader must never be able to mistake the two — so the label rides on the tile, not on the block alone.
//   * A tile that could not be solved prints the REASON in place of the number — stale last trade, no time value,
//     price below intrinsic, expiry today, non-convergence — in the server's own sentence. Never a blank.
//   * A reading with no volatility is a GAP in the line: the path breaks and starts again after it. Nothing is
//     interpolated, nothing is carried forward and nothing is drawn at zero.
//   * A strike the exchange does not list KEEPS ITS SLOT and says so, so the grid never shifts under the reader.
//   * Pointing at a tile lights that strike wherever else it is on this tab, exactly as a ΔOI tile does.
//
// The direction words are the owner's own — Expanding · Stable · Cooling — and they are past tense readings of
// captured 15-min readings. Nothing here says what any of it does next (§5).
import React,{useMemo} from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import Svg,{Path,Line,Circle,Text as SvgText} from 'react-native-svg';
import {C,T,s} from '../ui';
import {useDerivativeRead,type Read} from './useDerivatives';
import {Tag,head,typeColor,buildupColor} from './frame';
import {TILE_H,gridColumns} from './OiGridSection';
import {DASH,IV_CHIPS,IV_COMPUTED_TAG,IV_KEYS,axisTimes,ivLatestRefusal,ivText,linePath,optionTone,
 sessionChip,sessionTone,servedDirection,strike as strikeText,type Scaled} from './logic';
import type {IvLeg,IvLegPoint,IvSeries,GridSlot,OiGrid} from './types';

/** The ten slots, in the ONE order both grids use. Built here rather than read off the response, so a short
 *  answer from the server leaves a named hole instead of shifting the tiles under the reader. */
export const IV_GRID_ORDER=['CE+0','CE+1','CE+2','CE+3','CE+4','PE+0','PE-1','PE-2','PE-3','PE-4'] as const;
export const IV_GRID_SLOTS=IV_GRID_ORDER.length;
export const IV_GRID_CALLS_LABEL='Calls — at the money and above';
export const IV_GRID_PUTS_LABEL='Puts — at the money and below';
/** Said once, in the block's one disclosure, about all ten tiles at once. */
export const IV_GRID_TEXT='Each tile is one contract\'s implied volatility through the session, on the same ten strikes the ΔOI grid draws and in the same order, so the two blocks can be read tile for tile.';
export const IV_GRID_GAP_TEXT='A 15-min reading the model could not solve is a gap in that tile\'s line, with the server\'s reason under it; nothing here is interpolated, carried forward or drawn at zero.';
export const IV_GRID_HIGHLIGHT_TEXT='Pointing at a tile lights the same strike wherever else it is on this tab — on the option chain, on the OI-by-strike bars and on the ΔOI grid. It highlights; it changes nothing.';
export const IV_GRID_COMPUTED_TEXT='Every tile carries COMPUTED because every number on it is a model\'s output, not a figure the exchange reported.';
/** The sentence a slot with no volatility anywhere in the session falls back to, when the server sent no reason. */
export const IV_GRID_NO_VALUE='No implied volatility has been solved for this contract at any reading of this session.';
/** The sentence a tile shows before the ladder is known. */
export const IV_GRID_NO_LADDER='The at-the-money ladder has not been resolved for this symbol yet.';

/** The requested leg travels back under `strike` as a LEG, not as a number — the shape `IvSeries` describes for
 *  the at-the-money pair. Declared here rather than in the shared types file, which another hand is in. */
export type IvStrikeSeries=Omit<IvSeries,'strike'>&{strike:IvLeg|null};

// --- geometry: the ΔOI tile's, to the pixel ----------------------------------------------------------------------
// TILE_H and the wrap points are IMPORTED from the ΔOI grid rather than copied, so the two blocks cannot drift
// apart: change one and both move. The zones inside a tile are laid out to the same rhythm, and the chart takes
// whatever TILE_H has left over — which makes "both rows are the same height" arithmetic rather than a promise.
const AXIS_W=34,TIME_H=12,CELL_GAP=8;
const TITLE_H=16,DETAIL_H=13,CHIP_H=15,STATE_H=14,REASON_H=26,CELL_PAD=8,ROW_GAP=4,BORDER=2;
export const IV_CHART_H=TILE_H-(CELL_PAD*2+BORDER+ROW_GAP*5+TITLE_H+DETAIL_H+CHIP_H+STATE_H+REASON_H);
const IV_PLOT_H=IV_CHART_H-TIME_H;
const IV_WIDTH=1.4;

/** Maps a leg's implied volatility onto a box. A reading with no volatility stays NULL, so `linePath` breaks the
 *  line there: a gap is a gap, never a straight run through the missing readings. */
export function scaleIv(points:IvLegPoint[]|null|undefined,width:number,height:number):Scaled|null{
 const values=(points||[]).map(p=>{
  const v=p?.iv_pct??p?.iv;
  if(typeof v!=='number'||!Number.isFinite(v))return null;
  return Math.abs(v)<=3?v*100:v;
 });
 const real=values.filter((v):v is number=>v!=null);
 if(real.length<2||width<=0||height<=0)return null;
 const lo=Math.min(...real),hi=Math.max(...real),span=(hi-lo)||Math.abs(hi)||1;
 const step=values.length>1?width/(values.length-1):0;
 return {lo,hi,points:values.map((v,i)=>v==null?null:{x:i*step,y:height-((v-lo)/span)*height,i})};
}
/** The y labels of one tile: the highest and lowest solved volatility, where they do not collide. */
export function ivAxis(scaled:Scaled|null,height:number){
 if(!scaled)return [] as {v:number;y:number}[];
 const span=(scaled.hi-scaled.lo)||1;
 const out:{v:number;y:number}[]=[];
 for(const v of [scaled.hi,scaled.lo]){
  const y=height-((v-scaled.lo)/span)*height;
  if(!out.some(t=>Math.abs(t.y-y)<9))out.push({v,y});
 }
 return out;
}
/** "ATM+2 CE · 9 of 26 readings solved" — where the slot sits, and how much of the session it actually has. A
 *  line drawn through four readings out of twenty is not the same picture as one drawn through twenty. */
export function ivSlotDetail(slot:GridSlot,leg:IvLeg|null){
 if(!slot.present)return slot.label;
 const points=leg?.points||[];
 if(!points.length)return slot.label;
 const solved=points.filter(p=>p&&(p.iv??p.iv_pct)!=null).length;
 return `${slot.label} · ${solved} of ${points.length} reading${points.length===1?'':'s'} solved`;
}
/** What a screen reader hears for one tile. Describes what was captured and solved; never what comes next. */
export function ivSlotSpoken(slot:GridSlot,leg:IvLeg|null,value:unknown,chip:string,reason:string){
 const what=`${strikeText(slot.strike)===DASH?slot.label:`${strikeText(slot.strike)} ${slot.option_type}`}, ${slot.label}`;
 if(!slot.present)return `${what}. ${slot.missing_text||'This strike is not listed in this expiry.'}`;
 if(value==null)return `${what}. No computed implied volatility. ${reason}`;
 const state=chip?chip.replace(/^[^ ]+ /,'').toLowerCase():'no baseline';
 return `${what}. Computed implied volatility ${ivText(value)}, ${state}. `
  +`${ivSlotDetail(slot,leg).split(' · ').slice(1).join(' · ')||''}`.trim();
}

// --- one tile ------------------------------------------------------------------------------------------------
function IvCell({slot,underlying,expiry,seq,width,selected,lit,onPick,onHighlight}:{slot:GridSlot;
 underlying:string;expiry:string;seq:number;width:number;selected?:(leg:IvLeg|null)=>boolean;lit?:boolean;
 onPick?:(leg:IvLeg,point:IvLegPoint|null)=>void;onHighlight?:(strike:number|null)=>void}){
 // One read per tile, and only for a strike the exchange actually lists: an unlisted slot asks the server
 // nothing and prints the reason it is empty.
 const path=underlying&&slot.present&&slot.strike!=null
  ?`/api/derivatives/iv-series?underlying=${encodeURIComponent(underlying)}`
   +`${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`
   +`&strike=${encodeURIComponent(String(slot.strike))}&option_type=${encodeURIComponent(slot.option_type)}`
  :null;
 const read=useDerivativeRead<IvStrikeSeries>(path,seq);
 const body=read.data;
 const leg=body?.strike||null;
 const points=leg?.points||[];
 const values=useMemo(()=>points.map(p=>{
  const v=p?.iv_pct??p?.iv;
  return typeof v==='number'&&Number.isFinite(v)?v:null;
 }),[points]);
 const last=[...points].reverse().find(p=>p&&(p.iv??p.iv_pct)!=null)||null;
 const value=last?(last.iv_pct??last.iv):null;
 // The direction the SERVER sent when it sent one, and the one read off the very points this tile drew when it
 // did not — so a chip can never disagree with its own line. The words are the owner's: Expanding · Stable · Cooling.
 const direction=servedDirection(leg?.direction,values,IV_KEYS);
 const chip=sessionChip(IV_CHIPS,direction),chipTone=sessionTone(IV_CHIPS,direction);
 // A null is NEVER a blank: the server's own reason stands where the number would have been.
 const refusal=value!=null?''
  :(ivLatestRefusal(points,body?.reason_text)||String(leg?.missing_text||'')
    ||String(slot.missing_text||'')||(read.phase==='loading'?'':IV_GRID_NO_VALUE));
 const note=!slot.present?(slot.missing_text||'This strike is not listed in this expiry.')
  :!points.length?(read.phase==='loading'?'':IV_GRID_NO_VALUE)
  :values.filter(v=>v!=null).length<2?(refusal||'One solved reading is a dot, not a line.')
  :'';
 const colour=typeColor(optionTone(slot.option_type));
 const plotW=Math.max(0,width-AXIS_W-22);
 const scaled=scaleIv(points,plotW,IV_PLOT_H);
 const axis=ivAxis(scaled,IV_PLOT_H),times=axisTimes(points as any,3);
 const tip=scaled?[...scaled.points].reverse().find(Boolean):null;
 const title=strikeText(slot.strike)===DASH?slot.label:`${strikeText(slot.strike)} ${slot.option_type}`;
 const pick=()=>{if(leg&&onPick)onPick(leg,last)};
 const on=!!selected?.(leg);
 return <Pressable accessibilityRole="button" accessibilityState={{selected:on}}
  accessibilityLabel={ivSlotSpoken(slot,leg,value,chip,refusal)}
  disabled={!slot.present||!leg||!onPick} onPress={pick}
  onHoverIn={()=>onHighlight?.(slot.strike==null?null:Number(slot.strike))}
  onHoverOut={()=>onHighlight?.(null)}
  onFocus={()=>onHighlight?.(slot.strike==null?null:Number(slot.strike))}
  onBlur={()=>onHighlight?.(null)}
  style={(st:any)=>[{width,height:TILE_H,padding:CELL_PAD,borderRadius:10,gap:ROW_GAP,
   borderWidth:on||st.focused?2:1,
   borderColor:on||st.focused?C.green:lit?C.mint:C.line,
   backgroundColor:on?C.soft:lit||st.hovered||st.focused?C.dark:'transparent'}]}>
  {/* THE MARKING, on every tile: this number is a model's output, not one the exchange reported. It is here
      whether or not the tile solved, because what it labels is the tile, not the figure. */}
  <View style={[s.row,{gap:5,height:TITLE_H}]}>
   <T numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:12,lineHeight:TITLE_H,color:colour,
    flexShrink:1,minWidth:0}}>{title}</T>
   <Tag label={IV_COMPUTED_TAG} a11y="Computed by a pricing model, not reported by the exchange"/>
  </View>
  <T numberOfLines={1} style={{fontSize:9,lineHeight:DETAIL_H,height:DETAIL_H,color:C.muted}}>
   {ivSlotDetail(slot,leg)}</T>
  <View style={{height:IV_CHART_H}}>
   {note?<View style={{flex:1,justifyContent:'center'}}>
     <T style={{fontSize:9,lineHeight:13,color:C.muted}}>{note}</T></View>
   :plotW>0&&<Svg width={width-18} height={IV_CHART_H}>
    {axis.map(t=><SvgText key={`y${t.v}`} x={0} y={Math.min(IV_CHART_H-TIME_H,Math.max(8,t.y+3))} fill={C.muted}
     fontSize={8}>{ivText(t.v)}</SvgText>)}
    {!!scaled&&<Line x1={AXIS_W} x2={AXIS_W+plotW} y1={IV_PLOT_H} y2={IV_PLOT_H} stroke={C.line} strokeWidth={1}
     strokeDasharray="2 3"/>}
    {/* linePath starts a new sub-path after every null, so an unsolved reading is a HOLE in the line */}
    <Path d={linePath(scaled)} stroke={colour} strokeWidth={IV_WIDTH} fill="none"
     transform={`translate(${AXIS_W},0)`}/>
    {!!tip&&<Circle cx={AXIS_W+tip.x} cy={tip.y} r={2.4} fill={colour}/>}
    {times.map(t=>{
     const step=points.length>1?plotW/(points.length-1):0;
     return <SvgText key={`x${t.i}`} x={Math.min(width-40,AXIS_W+t.i*step-10)} y={IV_CHART_H-2} fill={C.muted}
      fontSize={8}>{t.label}</SvgText>;
    })}
   </Svg>}
  </View>
  {/* the chip on the left and the volatility hard right, on every tile, so a row reads as a column of numbers */}
  <View style={[s.row,{gap:6,height:CHIP_H}]}>
   {chip?<View style={{paddingHorizontal:5,paddingVertical:1,borderRadius:4,borderWidth:1,
     borderColor:chipTone==='flat'?C.line:buildupColor(chipTone)}}>
     <T style={{fontSize:9,lineHeight:13,fontFamily:'InterSemi',
      color:chipTone==='flat'?C.muted:buildupColor(chipTone)}}>{chip}</T></View>
   :<T style={{fontSize:9,lineHeight:13,color:C.muted}}>no baseline</T>}
   <View style={{flex:1}}/>
   <T numberOfLines={1} style={{fontSize:9,lineHeight:13,color:value==null?C.muted:C.ink,
    fontVariant:['tabular-nums']}}>{ivText(value)}</T>
   <Tag label={IV_COMPUTED_TAG}/>
  </View>
  {/* plain text, never a coloured chip: a description of what the model did, not a signal. Both lines keep a
      box of their own whether or not there is anything to put in them, so the two rows stay in step. */}
  <T numberOfLines={1} style={{fontSize:9,lineHeight:13,height:STATE_H,fontFamily:'InterMedium',
   color:value==null?C.muted:C.ink}}>
   {value==null?'No volatility at the latest reading':'Solved at the latest reading'}</T>
  <View style={{height:REASON_H}}>
   <T numberOfLines={2} style={{fontSize:9,lineHeight:13,color:C.muted}}>{refusal}</T></View>
 </Pressable>;
}

// --- the 2 x 5 grid ---------------------------------------------------------------------------------------------
/** `onPick` keeps the leg-and-point signature the strike list used, so a tile re-points the tab exactly as a row
 *  of that list did: a strike is a symbol choice like any other row on this tab. */
 export type IvStrikeGridProps={underlying:string;expiry:string;seq:number;
 /** The TAB's one ΔOI-grid read, made in index.tsx and shared with the ΔOI block and the signal table.
  *  This grid used to fetch the ladder itself, which was an eleventh request for a payload the page already
  *  had - and two answers that could drift apart at a refresh. The ten per-strike IV reads below are
  *  inherent; a second ladder was not. */
 ladder:Read<OiGrid>;
 onPick?:(leg:IvLeg,point:IvLegPoint|null)=>void;
 selected?:(leg:IvLeg|null)=>boolean;
 highlight?:number|null;onHighlight?:(strike:number|null)=>void};
export function IvStrikeGrid({underlying,expiry,seq,ladder,onPick,selected,highlight,onHighlight}:IvStrikeGridProps){
 const [width,setWidth]=React.useState(0);
 // The LADDER is the TAB's, handed down: the same ten contracts as the ΔOI grid and the signal table, by
 // construction rather than by coincidence, and fetched once for all three.
 const rows=ladder.data?.rows||[];
 // TEN slots, always, in ONE order. A slot the server did not send is still a slot and says so, so nothing
 // shifts under the reader between one reading and the next.
 const slots=useMemo(()=>{
  const by=new Map<string,GridSlot>();
  for(const row of rows)if(row?.slot)by.set(row.slot,row);
  return IV_GRID_ORDER.map(key=>{
   const found=by.get(key);
   if(found)return found;
   const kind=key.slice(0,2) as 'CE'|'PE';
   const offset=Number(key.slice(2))||0;
   const label=`ATM${offset===0?'':offset>0?`+${offset}`:`−${Math.abs(offset)}`} ${kind}`;
   return {slot:key,option_type:kind,atm_offset:offset,label,row:kind==='CE'?'calls':'puts',present:false,
    tradingsymbol:null,instrument_token:null,strike:null,previous_close_oi:null,points:[],
    direction:'no baseline',direction_detail:{},marks:0,marks_with_delta:0,latest_delta_oi:null,
    peak_abs_delta_oi:null,marks_with_price:0,latest_price:null,
    flow:{price_direction:'no baseline',oi_direction:'no baseline',what_label:'',meaning:null,detail:{}},
    missing_text:underlying?IV_GRID_NO_LADDER:''} as GridSlot;
  });
 },[rows,underlying]);
 const bands=[{key:'calls',label:IV_GRID_CALLS_LABEL,slots:slots.slice(0,IV_GRID_SLOTS/2)},
  {key:'puts',label:IV_GRID_PUTS_LABEL,slots:slots.slice(IV_GRID_SLOTS/2)}];
 const columns=gridColumns(width||660);
 const cell=Math.max(120,Math.floor(((width||660)-CELL_GAP*(columns-1))/columns));
 return <View onLayout={e=>{const w=Math.round(e.nativeEvent.layout.width);setWidth(v=>Math.abs(v-w)<3?v:w)}}
  style={{gap:10}}>
  {bands.map(band=><View key={band.key} style={{gap:6}}>
   <T style={head}>{band.label}</T>
   {/* every tile is TILE_H tall, so the calls row and the puts row are the same height to the pixel */}
   <View style={[s.row,{flexWrap:'wrap',alignItems:'flex-start',gap:CELL_GAP}]}>
    {band.slots.map(slot=><IvCell key={slot.slot} slot={slot} underlying={underlying} expiry={expiry} seq={seq}
     width={cell} onPick={onPick} onHighlight={onHighlight} selected={selected}
     lit={highlight!=null&&slot.strike!=null&&Math.abs(Number(slot.strike)-highlight)<0.5}/>)}
   </View>
  </View>)}
 </View>;
}
