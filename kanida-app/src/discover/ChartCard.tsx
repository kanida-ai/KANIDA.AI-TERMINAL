// Chart card (spec §3.5): the linked stock's stored setup drawn by PatternCanvas. Key-guarded on the block link; a response for an older link is never shown.
// Dense card: two-line header (SYMBOL · TF + Open full chart → / source badge + strategy), chart fills the body without its control row, one footer line.
import React,{useCallback,useState} from 'react';
import {View} from 'react-native';
import {router} from 'expo-router';
import {C,T,s} from '../ui';
import {api,type Match} from '../model';
import {useProduct} from '../context';
import {useKeyedValue} from '../activeSymbol';
import {PatternCanvas} from '../PatternCanvas';
import {WidgetError,formatDataDate} from '../layout';
import {cardDataLine,cardWarning} from '../layout/dataStatus';
import {linkKey,type BlockLink} from './link';
import {chartHref} from './deeplink';
import {CardShell,CenterNote,LinkText,Skeleton} from './parts';
import {geometryNoteText} from './cardLogic';
export const openChart=(l:BlockLink,tab?:string)=>{const href=chartHref(l.symbol,l.timeframe,l.matchId,tab,l.detectionId);if(href)router.push(href as any)};
/** "29 Jul 2026" → "29 Jul" in the current year; other years keep the year. */
export const cardDate=(v?:string|null)=>{const d=formatDataDate(v||undefined);if(!d)return '';const y=String(new Date().getFullYear());return d.endsWith(' '+y)?d.slice(0,-5):d;};
/** Strategy name without its timeframe part ("Falling Wedge breakout · 1D" → "Falling Wedge breakout"). */
export const strategyShort=(l:BlockLink)=>l.strategyName.replace(` · ${l.timeframe}`,'')||l.patternName;
/** Two-line linked-card header shared by Chart and Backtest: line 1 bold title (never truncated) + link on the right; line 2 slot badge in the block colour + muted detail. */
export function LinkedHeader({title,detail,slot,color,sourceA11y,link}:{title:string;detail:string;slot?:string;color?:string;sourceA11y?:string;link?:React.ReactNode}){
 return <View style={{flex:1,minWidth:0,paddingVertical:3,paddingRight:8}}>
  <View style={[s.row,{gap:8,minHeight:26}]}>
   <T role="heading" aria-level={3} style={{flexShrink:0,fontFamily:'InterSemi',fontSize:13,lineHeight:18}}>{title}</T>
   <View style={{flex:1}}/>
   {!!link&&<View style={{flexShrink:1,minWidth:0}}>{link}</View>}
  </View>
  <View accessibilityLabel={sourceA11y} style={[s.row,{gap:5,minWidth:0}]}>
   {!!slot&&<View aria-hidden style={{minWidth:16,height:16,paddingHorizontal:3,borderRadius:4,alignItems:'center',justifyContent:'center',backgroundColor:color||C.green}}><T style={{fontSize:10,lineHeight:12,fontFamily:'InterSemi',color:'#041B12'}}>{slot}</T></View>}
   <T numberOfLines={1} style={{flexShrink:1,minWidth:0,fontSize:11,lineHeight:15,color:C.muted}}>{detail}</T>
  </View>
 </View>;
}
export const EmptyHeader=({title}:{title:string})=><T role="heading" aria-level={3} numberOfLines={1} style={{flex:1,fontFamily:'InterSemi',fontSize:12,lineHeight:16,letterSpacing:1,textTransform:'uppercase'}}>{title}</T>;
export function ChartCard({link,style,stale,ageDays,color}:{link:BlockLink|null;style?:any;stale?:boolean;ageDays?:number|null;color?:string}){
 const p=useProduct(),key=linkKey(link);
 const {value:match,error,loading,reload}=useKeyedValue<Match|null>(key,async()=>{
  const l=link!;
  // A LIVE research detection is already fully identified by the link; PatternCanvas fetches /api/chart once
  // and finds THIS episode by its detection id, so there is no second list request and no pattern-name guess.
  if(l.detectionId)return {id:l.matchId,symbol:l.symbol,company:l.company,price:0,pattern:l.pattern,pattern_name:l.patternName,
   timeframe:l.timeframe,direction:l.side==='long'?'bullish':'bearish',state:l.detectionState||'',current:true,
   candle_end:l.dataEnd||'',score:0,sector:'',universes:[],history:[],start_index:0} as Match;
  const local=(Array.isArray(p?.matches)?p!.matches:[]).find((m:Match)=>m.id===l.matchId);if(local)return local;
  const list=await api(`/api/matches?min_trades=0&symbol=${encodeURIComponent(l.symbol)}&timeframe=${encodeURIComponent(l.timeframe)}&pattern=${encodeURIComponent(l.pattern)}`);
  const arr:Match[]=Array.isArray(list)?list:Array.isArray(list?.matches)?list.matches:[];
  return arr.find(m=>m.id===l.matchId)||arr.find(m=>m.symbol===l.symbol&&m.timeframe===l.timeframe&&m.pattern===l.pattern)||null;
 });
 // The drawing status stays available to assistive tech (footer label) without a visible status row.
 const [status,setStatus]=useState<{key:string;text:string}>({key:'',text:''});
 const onStatus=useCallback((text:string)=>setStatus({key,text}),[key]);
 const title=link?`${link.symbol} · ${link.timeframe}`:'Chart';
 const header=link?<LinkedHeader title={title} slot={link.slot} color={color} detail={`${strategyShort(link)} · from ${link.slot}`} sourceA11y={`${link.company}. Source: ${link.strategyName}, scanner ${link.slot}`}
  link={<LinkText label="Open full chart →" a11y={`Open ${link.symbol} ${link.timeframe} on the full chart`} onPress={()=>openChart(link)}/>}/>:<EmptyHeader title="Chart"/>;
 // Same words as the header line: the candle this pattern was read on, and the newest price time.
 const dataStatus=p?.state?.data_status,dataLine=link?cardDataLine(dataStatus,link.dataEnd):'',warn=link?cardWarning(dataStatus,{stale,ageDays}):null;
 const note=link?geometryNoteText({drawable:link.drawable,geometry_note:link.geometryNote}):null;
 const footer=link?<T numberOfLines={1} accessibilityLabel={`${dataLine}. Times are IST.${warn?` ${warn}.`:''}${status.key===key&&status.text?` ${status.text}`:''}`} style={{fontSize:11,lineHeight:15,color:C.muted}}>
  {dataLine}{warn?<T style={{fontSize:11,lineHeight:15,color:C.amber}}> · {warn}</T>:' · IST'}
 </T>:null;
 let body:React.ReactNode;
 if(!link)body=<CenterNote text="Click a stock in a scanner card to see its chart."/>;
 else if(error)body=<View style={{padding:12}}><WidgetError title="Chart unavailable" message={`The stored setup for ${link.symbol} ${link.timeframe} could not be loaded. ${error}`} onRetry={reload}/></View>;
 else if(loading)body=<Skeleton chart lines={2}/>;
 else if(!match)body=<CenterNote icon="alert-circle" text={link.detectionId?`This ${link.patternName} detection for ${link.symbol} ${link.timeframe} is no longer in the live scan.`:`The stored ${link.patternName} setup for ${link.symbol} ${link.timeframe} is no longer in the stored scan.`}/>;
 else body=<View style={{flex:1,minHeight:0,padding:6}}>
  {/* Non-empty exactly when the detector published nothing drawable (pattern_lines.build's lines/note
      exclusivity). The card says so above the candles instead of showing an outline-less chart in silence. */}
  {!!note&&<T accessibilityLiveRegion="polite" numberOfLines={2} style={{fontSize:11,lineHeight:15,color:C.amber,paddingHorizontal:4,paddingBottom:4}}>Marker only — {note}</T>}
  <PatternCanvas key={key} match={match} side={link.side} detectionId={link.detectionId||null} fill hideHeader compact controls={false} onStatus={onStatus}/>
 </View>;
 return <CardShell label={link?`Chart, ${link.symbol} · ${link.patternName} · ${link.timeframe}`:'Chart'} style={style} header={header} footer={footer}>{body}</CardShell>;
}
