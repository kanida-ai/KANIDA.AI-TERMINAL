import React from 'react';
import {View,Pressable,Platform} from 'react-native';
import {useRouter} from 'expo-router';
import {C,T,Icon} from '../ui';
import type {Insight,InsightKind} from '../strategies/types';
// Insight card: server text only (headline, why, chips). Whole card opens the Discover deep link. Win rate, if present, is inside the server's `why` text as context.
const KIND:Record<InsightKind,{label:string;color:string;bg:string}>={setup:{label:'Setup',color:C.green,bg:C.soft},caution:{label:'Caution',color:C.amber,bg:C.amberBg},cluster:{label:'Cluster',color:C.mint,bg:'#10232A'},context:{label:'Context',color:C.muted,bg:C.bg}};
export function InsightCard({insight,staleDays}:{insight:Insight;staleDays:number|null}){
 const router=useRouter();const k=KIND[insight.kind]??KIND.context;const web=Platform.OS==='web';
 const open=()=>{if(insight.deeplink)router.push(insight.deeplink as any);};
 return <Pressable accessibilityRole="link" accessibilityLabel={`${insight.rank}. ${k.label}: ${insight.headline}. ${insight.why}${staleDays!=null?`. Research only, prices ${staleDays} days old`:''}. Open in Discover Strategies`} onPress={open} disabled={!insight.deeplink}
  style={(st:any)=>[{backgroundColor:st.hovered?'#0F1D26':C.paper,borderWidth:1,borderColor:st.focused?C.green:st.hovered?'#2A4150':C.line,borderRadius:14,padding:16,gap:9,opacity:st.pressed?.8:1},web&&st.focused?({outlineWidth:2,outlineStyle:'solid',outlineColor:C.green,outlineOffset:2} as any):null,web?({cursor:'pointer'} as any):null]}>
  {(st:any)=><>
   <View style={{flexDirection:'row',alignItems:'center',gap:8}}>
    <T style={{fontFamily:'ManropeBold',fontSize:15,color:C.muted,minWidth:16}}>{insight.rank}</T>
    <View style={{backgroundColor:k.bg,borderRadius:6,paddingHorizontal:8,paddingVertical:3}}><T style={{fontSize:10,lineHeight:15,fontFamily:'InterSemi',letterSpacing:.6,textTransform:'uppercase',color:k.color}}>{k.label}</T></View>
   </View>
   <T style={{fontFamily:'InterSemi',fontSize:15,lineHeight:22}}>{insight.headline}</T>
   {!!insight.why&&<T style={{fontSize:12.5,lineHeight:19,color:C.muted}}>{insight.why}</T>}
   {!!insight.chips?.length&&<View style={{flexDirection:'row',flexWrap:'wrap',gap:6}}>{insight.chips.map((c,i)=><View key={i} style={{borderWidth:1,borderColor:C.line,borderRadius:6,paddingHorizontal:7,paddingVertical:2}}><T style={{fontSize:11,lineHeight:16,color:C.muted}}>{c}</T></View>)}</View>}
   {staleDays!=null&&<View style={{flexDirection:'row',alignItems:'center',gap:6}}><Icon name="alert-triangle" size={12} color={C.amber}/><T style={{fontSize:11.5,color:C.amber}}>Research only — prices {staleDays} days old</T></View>}
   {!!insight.deeplink&&<View style={{flexDirection:'row',alignItems:'center',gap:6,paddingTop:2}}><T style={{fontSize:12.5,fontFamily:'InterSemi',color:C.green,textDecorationLine:st.hovered||st.focused?'underline':'none'}}>Open in Discover Strategies</T><Icon name="arrow-right" size={14} color={C.green}/></View>}
  </>}
 </Pressable>;
}
