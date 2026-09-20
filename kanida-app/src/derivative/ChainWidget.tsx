// Section 2 — Option chain for the chosen underlying and expiry (§4 card 2), unchanged in substance: OI, OI change,
// premium and the §3.1 build-up label, calls on the left and puts on the right, in the new widget frame. Clicking
// either side re-points the chart tile.
import React,{useEffect,useMemo,useRef} from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import {C,Icon,T,s} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {WidgetFrame,metaText,stateOf,head,tab,tone,buildupColor,webOnly} from './frame';
import {CHAIN_AT_MONEY_TEXT,CHAIN_NO_SPOT_TEXT,asOfText,buildupLabel,buildupTone,chainStartRow,compact,crore,
 dteText,floorsText,nearestStrikeIndex,price,signedUnits,strike as strikeText,
 volumeRatioShort} from './logic';
import type {Chain,ChainRow,ContractRow,ChartTarget} from './types';
const COL={oi:52,prem:58};
/** One row, one height. The chain has to be able to SCROLL ITSELF to the strike at the money, and an offset
 *  can only be worked out from a row height that does not depend on what a row happens to say. */
const ROW_H=42;
function Side({row,side,selected,required,onPress}:{row:ContractRow|null;side:'CE'|'PE';selected:boolean;
 required:number;onPress:(r:ContractRow)=>void}){
 if(!row)return <View style={{flex:1,minHeight:38,justifyContent:'center',paddingHorizontal:6}}>
  <T style={{fontSize:11,color:C.muted}}>—</T></View>;
 const label=`${side==='CE'?'Call':'Put'} ${strikeText(row.strike)}. Open interest ${compact(row.oi)}, change ${signedUnits(row.oi_change_day)}, premium ${crore(row.premium_cr)}, ${buildupLabel(row.buildup_day)} on the day, volume ${volumeRatioShort(row,required)} of its own median`;
 return <Pressable role="option" aria-selected={selected} accessibilityLabel={label} {...webOnly({tabIndex:0})}
  onPress={()=>onPress(row)}
  style={(st:any)=>[s.row,{flex:1,gap:5,minHeight:38,paddingHorizontal:6,borderRadius:7,
   borderWidth:selected||st.focused?2:1,
   borderColor:selected||st.focused?C.green:'transparent',
   backgroundColor:selected?C.soft:st.hovered||st.focused?'#0F1F28':'transparent'}]}>
  <View style={{flex:1,minWidth:0}}>
   <T numberOfLines={1} style={[tab,{fontSize:12,lineHeight:16}]}>{price(row.last_price)}</T>
   <T numberOfLines={1} style={{fontSize:9,lineHeight:12,color:buildupColor(buildupTone(row.buildup_day))}}>{buildupLabel(row.buildup_day)}</T>
  </View>
  <View style={{width:COL.oi,alignItems:'flex-end'}}>
   <T style={[tab,{fontSize:11,lineHeight:15}]}>{compact(row.oi)}</T>
   <T style={[tab,{fontSize:9,lineHeight:12,color:tone(row.oi_change_day)}]}>{signedUnits(row.oi_change_day)}</T>
  </View>
  <View style={{width:COL.prem,alignItems:'flex-end'}}>
   <T style={[tab,{fontSize:11,lineHeight:15}]}>{crore(row.premium_cr)}</T>
   <T style={[tab,{fontSize:9,lineHeight:12,color:C.muted}]}>{volumeRatioShort(row,required)}</T>
  </View>
 </Pressable>;
}
export type ChainWidgetProps={underlying:string;expiry:string;seq:number;target:ChartTarget|null;
 onTarget:(t:ChartTarget)=>void;filterCount?:number;onCustomize?:()=>void;onExpand?:()=>void;expanded?:boolean;
 onClose?:()=>void;
 /** The 15-min reading the TAB is on. Not decoration: a session rebuilt from 15-minute candles carries NO
  *  SPOT, so a chain that took the newest reading had no at-the-money strike to open at and started at its
  *  lowest - 21,350 against a spot of 23,302, every visible contract far out of the money. */
 at?:string;
 /** The strike under the pointer anywhere on the tab. Lighting it here is what links the chain to the bars
  *  and to the delta-OI grid: three panels about the same strikes that used to share no visual cue at all. */
 highlight?:number|null;onHighlight?:(strike:number|null)=>void;
 style?:any};
export function ChainWidget({underlying,expiry,at,seq,target,onTarget,filterCount,onCustomize,onExpand,
 expanded,onClose,highlight,onHighlight,style}:ChainWidgetProps){
 const path=underlying?`/api/derivatives/chain?underlying=${encodeURIComponent(underlying)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}${at?`&at=${encodeURIComponent(at)}`:''}`:null;
 const read=useDerivativeRead<Chain>(path,seq);
 const body=read.data,state=stateOf(read,'No strike of this expiry has been captured yet.');
 const required=body?.baseline_sessions_required??3;
 const pick=(row:ContractRow)=>onTarget({underlying:row.underlying,instrumentToken:row.instrument_token,
  label:row.tradingsymbol||row.underlying,
  detail:`${strikeText(row.strike)} ${row.instrument_type} · ${dteText(row.days_to_expiry)}`});
 // WHERE THE CHAIN OPENS. The ladder is whole and in strike order - nothing is dropped and nothing is
 // reordered - but the reader lands at the money instead of at the lowest strike listed. When the reading
 // carries no spot there is no at-the-money strike to land on, and the panel says that rather than pretending.
 const rows=body?.rows||[];
 const atmIndex=useMemo(()=>nearestStrikeIndex(rows,body?.spot),[rows,body?.spot]);
 const list=useRef<any>(null);
 const jump=React.useCallback((animated:boolean)=>{
  if(atmIndex==null)return;
  list.current?.scrollTo?.({y:chainStartRow(rows,body?.spot)*ROW_H,animated});
 },[atmIndex,rows,body?.spot]);
 // on arrival, and whenever the chain becomes a different chain
 useEffect(()=>{jump(false)},[underlying,body?.expiry,atmIndex,jump]);
 const subtitle=underlying?`${underlying}${body?.expiry?` · ${body.expiry}`:''}${body?.days_to_expiry!=null?` · ${dteText(body.days_to_expiry)}`:''}${body?.spot!=null?` · spot ${price(body.spot)}`:''}`:'';
 if(!underlying)return <WidgetFrame name="Option chain" body={null} filterCount={filterCount} onCustomize={onCustomize}
  onClose={onClose} state={{phase:'empty',text:'Click any row on the screener above, or choose a symbol in Customize, to see a chain.'}}
  footer={null} style={style}/>;
 return <WidgetFrame name="Option chain" subtitle={subtitle} body={body} state={state} onRefresh={read.reload}
  filterCount={filterCount} onCustomize={onCustomize} onExpand={onExpand} expanded={expanded} onClose={onClose}
  showsSignals={['oi_change_day','buildup_day','premium_cr','volume_ratio']} style={style}
  toolbar={atmIndex==null?undefined:<View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
   <Pressable accessibilityRole="button"
    accessibilityLabel={`Scroll the chain back to ${strikeText(rows[atmIndex]?.strike)}, the strike nearest the captured spot`}
    onPress={()=>jump(true)}
    style={(st:any)=>[s.row,{gap:5,minHeight:24,paddingHorizontal:8,borderRadius:6,borderWidth:1,
     borderColor:st.hovered||st.focused?C.green:C.line,
     backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
    <Icon name="crosshair" size={11} color={C.mint}/>
    <T numberOfLines={1} style={{fontSize:10,lineHeight:14,color:C.mint}}>
     At the money {strikeText(rows[atmIndex]?.strike)}</T>
   </Pressable>
   <T style={metaText}>Spot {price(body?.spot)} at this 15-min reading</T>
  </View>}
  inBlock note={<View style={{gap:2}}>
   <T style={metaText}>{asOfText(body?.as_of)} · {floorsText(body?.floors,body?.floors_text)}</T>
   <T style={metaText}>{atmIndex==null?CHAIN_NO_SPOT_TEXT:CHAIN_AT_MONEY_TEXT}</T>
   <T style={metaText}>Build-up here is the day-on-day reading; the 15-minute reading is a separate label and is never mixed into it (§3.1). Pointing at a strike lights the same strike wherever else it is on this tab.</T>
  </View>}>
  <View style={[s.row,{paddingHorizontal:9,minHeight:22,gap:5}]}>
   <T style={[head,{flex:1}]}>Calls</T><T style={[head,{width:COL.oi,textAlign:'right'}]}>OI</T>
   <T style={[head,{width:COL.prem,textAlign:'right'}]}>Premium</T>
   <T style={[head,{width:58,textAlign:'center'}]}>Strike</T>
   <T style={[head,{flex:1}]}>Puts</T><T style={[head,{width:COL.oi,textAlign:'right'}]}>OI</T>
   <T style={[head,{width:COL.prem,textAlign:'right'}]}>Premium</T>
  </View>
  <ScrollView ref={list} style={{flex:1}} contentContainerStyle={{paddingBottom:8}}>
   <View {...webOnly({role:'listbox'})} aria-label={`Option chain for ${underlying}`}>
    {rows.map((row:ChainRow)=>{
     const atm=body?.spot!=null&&Math.abs(row.strike-body.spot)<=Math.max(1,(body.spot||0)*0.002);
     const lit=highlight!=null&&Math.abs(row.strike-highlight)<0.5;
     return <View key={row.strike}
      onPointerEnter={onHighlight?()=>onHighlight(row.strike):undefined}
      onPointerLeave={onHighlight?()=>onHighlight(null):undefined}
      style={[s.row,{gap:4,height:ROW_H,paddingHorizontal:3,borderRadius:8,borderWidth:1,
       borderColor:lit?C.mint:atm?C.mint:'transparent',
       backgroundColor:lit?C.dark:atm?'#0C1E1A':'transparent'}]}>
      <Side row={row.ce} side="CE" selected={!!row.ce&&target?.instrumentToken===row.ce.instrument_token}
       required={required} onPress={pick}/>
      <View style={{width:58,alignItems:'center',justifyContent:'center',paddingVertical:2}}>
       <T style={[tab,{fontSize:12,lineHeight:16,fontFamily:'InterSemi',
        color:lit?C.mint:atm?C.mint:C.ink}]}>{strikeText(row.strike)}</T>
      </View>
      <Side row={row.pe} side="PE" selected={!!row.pe&&target?.instrumentToken===row.pe.instrument_token}
       required={required} onPress={pick}/>
     </View>;
    })}
   </View>
  </ScrollView>
 </WidgetFrame>;
}
