// Section 2 — Option chain for the chosen underlying and expiry (§4 card 2), unchanged in substance: OI, OI change,
// premium and the §3.1 build-up label, calls on the left and puts on the right, in the new widget frame. Clicking
// either side re-points the chart tile.
import React from 'react';
import {View,ScrollView,Pressable} from 'react-native';
import {C,T,s} from '../ui';
import {useDerivativeRead} from './useDerivatives';
import {WidgetFrame,stateOf,head,tab,tone,buildupColor,webOnly} from './frame';
import {buildupLabel,buildupTone,compact,crore,dteText,price,signedUnits,strike as strikeText,
 volumeRatioShort} from './logic';
import type {Chain,ChainRow,ContractRow,ChartTarget} from './types';
const COL={oi:52,prem:58};
function Side({row,side,selected,required,onPress}:{row:ContractRow|null;side:'CE'|'PE';selected:boolean;
 required:number;onPress:(r:ContractRow)=>void}){
 if(!row)return <View style={{flex:1,minHeight:38,justifyContent:'center',paddingHorizontal:6}}>
  <T style={{fontSize:11,color:C.muted}}>—</T></View>;
 const label=`${side==='CE'?'Call':'Put'} ${strikeText(row.strike)}. Open interest ${compact(row.oi)}, change ${signedUnits(row.oi_change_day)}, premium ${crore(row.premium_cr)}, ${buildupLabel(row.buildup_day)} on the day, volume ${volumeRatioShort(row,required)} of its own median`;
 return <Pressable role="option" aria-selected={selected} accessibilityLabel={label} {...webOnly({tabIndex:0})}
  onPress={()=>onPress(row)}
  style={(st:any)=>[s.row,{flex:1,gap:5,minHeight:38,paddingHorizontal:6,borderRadius:7,borderWidth:1,
   borderColor:selected?C.green:'transparent',backgroundColor:selected?C.soft:st.hovered?'#0F1F28':'transparent'}]}>
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
 onClose?:()=>void;style?:any};
export function ChainWidget({underlying,expiry,seq,target,onTarget,filterCount,onCustomize,onExpand,expanded,
 onClose,style}:ChainWidgetProps){
 const path=underlying?`/api/derivatives/chain?underlying=${encodeURIComponent(underlying)}${expiry?`&expiry=${encodeURIComponent(expiry)}`:''}`:null;
 const read=useDerivativeRead<Chain>(path,seq);
 const body=read.data,state=stateOf(read,'No strike of this expiry has been captured yet.');
 const required=body?.baseline_sessions_required??3;
 const pick=(row:ContractRow)=>onTarget({underlying:row.underlying,instrumentToken:row.instrument_token,
  label:row.tradingsymbol||row.underlying,
  detail:`${strikeText(row.strike)} ${row.instrument_type} · ${dteText(row.days_to_expiry)}`});
 const subtitle=underlying?`${underlying}${body?.expiry?` · ${body.expiry}`:''}${body?.days_to_expiry!=null?` · ${dteText(body.days_to_expiry)}`:''}${body?.spot!=null?` · spot ${price(body.spot)}`:''}`:'';
 if(!underlying)return <WidgetFrame name="Option chain" body={null} filterCount={filterCount} onCustomize={onCustomize}
  onClose={onClose} state={{phase:'empty',text:'Add a "Symbol is …" filter, or click any row above, to see a chain.'}}
  footer={null} style={style}/>;
 return <WidgetFrame name="Option chain" subtitle={subtitle} body={body} state={state} onRefresh={read.reload}
  filterCount={filterCount} onCustomize={onCustomize} onExpand={onExpand} expanded={expanded} onClose={onClose}
  showsSignals={['oi_change_day','buildup_day','premium_cr','volume_ratio']} style={style}
  note="The tinted row is the strike nearest the captured spot. Build-up here is the day-on-day reading; the 15-minute reading is a separate label and is never mixed into it (§3.1).">
  <View style={[s.row,{paddingHorizontal:9,minHeight:22,gap:5}]}>
   <T style={[head,{flex:1}]}>Calls</T><T style={[head,{width:COL.oi,textAlign:'right'}]}>OI</T>
   <T style={[head,{width:COL.prem,textAlign:'right'}]}>Premium</T>
   <T style={[head,{width:58,textAlign:'center'}]}>Strike</T>
   <T style={[head,{flex:1}]}>Puts</T><T style={[head,{width:COL.oi,textAlign:'right'}]}>OI</T>
   <T style={[head,{width:COL.prem,textAlign:'right'}]}>Premium</T>
  </View>
  <ScrollView style={{flex:1}} contentContainerStyle={{paddingBottom:8}}>
   <View {...webOnly({role:'listbox'})} aria-label={`Option chain for ${underlying}`}>
    {(body?.rows||[]).map((row:ChainRow)=>{
     const atm=body?.spot!=null&&Math.abs(row.strike-body.spot)<=Math.max(1,(body.spot||0)*0.002);
     return <View key={row.strike} style={[s.row,{gap:4,paddingHorizontal:3,borderRadius:8,
      backgroundColor:atm?'#0C1E1A':'transparent'}]}>
      <Side row={row.ce} side="CE" selected={!!row.ce&&target?.instrumentToken===row.ce.instrument_token}
       required={required} onPress={pick}/>
      <View style={{width:58,alignItems:'center',justifyContent:'center',paddingVertical:2}}>
       <T style={[tab,{fontSize:12,lineHeight:16,fontFamily:'InterSemi',color:atm?C.mint:C.ink}]}>{strikeText(row.strike)}</T>
      </View>
      <Side row={row.pe} side="PE" selected={!!row.pe&&target?.instrumentToken===row.pe.instrument_token}
       required={required} onPress={pick}/>
     </View>;
    })}
   </View>
  </ScrollView>
 </WidgetFrame>;
}
