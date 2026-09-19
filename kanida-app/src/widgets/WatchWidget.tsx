import React,{useState} from 'react';
import {View,Pressable} from 'react-native';
import {router} from 'expo-router';
import {useProduct} from '../context';
import {C,T,s,Button,Badge,Sheet} from '../ui';
import {Match,api} from '../model';
import {decision,dataAgeDays,dataAgeText,isStale} from '../decision';
import {useActiveSymbol} from '../activeSymbol';
import {Widget,WidgetError,IconButton,type WidgetMenuItem,type SidebarWidget} from '../layout';
// Sidebar Watch widget (TRENDSPIDER_STUDY §10.1/§10.2) plus the watch row shared with the full /watch screen (WatchDesk), so both read the same status wording.
// Data: useProduct().product.watchlist (GET /api/product) and the stored scan in useProduct().matches. No new endpoints. A row writes the active-symbol store with source 'watch'.
export type WatchItem={id:string;symbol:string;pattern?:string;pattern_name:string;timeframe:string;direction?:string;candle_end?:string};
export type WatchStatus={match?:Match;badge:'PATTERN ABSENT'|'NEW SNAPSHOT'|'WAITING';tone:'red'|'amber';detail:string};
/** Joins a saved watch with the latest stored scan. Same text on /watch and in the sidebar. */
export function watchStatus(w:WatchItem,matches:Match[]):WatchStatus{const m=(matches||[]).find(x=>x.id===w.id);return {match:m,badge:!m?'PATTERN ABSENT':m.candle_end!==w.candle_end?'NEW SNAPSHOT':'WAITING',tone:!m?'red':'amber',detail:!m?'This pattern is no longer in the latest stored scan.':m.candle_end===w.candle_end?'The source candle has not changed since you added this setup.':'A new stored candle is available. Review the updated structure and history.'};}
/** Remove from watch behind the existing confirmation sheet. Render `sheet` once per screen. */
export function useWatchRemoval(){
 const {refreshProduct,setToast}=useProduct();const [removing,setRemoving]=useState<WatchItem|null>(null),[busy,setBusy]=useState(false);
 async function remove(){if(!removing)return;setBusy(true);try{await api('/api/product/watch',{action:'remove',match_id:removing.id});await refreshProduct();setToast('Removed from your watchlist.');setRemoving(null)}catch(e:any){setToast(e.message)}finally{setBusy(false)}}
 const sheet=<Sheet visible={!!removing} onClose={()=>{if(!busy)setRemoving(null)}} title={`Remove ${removing?.symbol||''} from watch?`}><T style={{fontSize:16,lineHeight:26}}>{removing?.pattern_name} · {removing?.timeframe} will leave your watch. Its stored history is not changed, and you can add it again while the setup appears in Discover.</T><Button label="Remove from watch" loading={busy} onPress={remove}/><Button label="Keep watching" kind="outline" disabled={busy} onPress={()=>setRemoving(null)}/></Sheet>;
 return {removing,setRemoving,busy,sheet};
}
export type WatchRowProps={item:WatchItem;variant?:'card'|'compact';onRemove:()=>void;onReview?:(m:Match)=>void;onSelect?:(item:WatchItem,m?:Match)=>void;selected?:boolean;marker?:boolean};
const FAINT='rgba(57,229,163,0.4)';
/** card = the /watch card (unchanged markup). compact = sidebar row: symbol · verdict chip / pattern · TF · data age of the stored snapshot. */
export function WatchRow({item:w,variant='card',onRemove,onReview,onSelect,selected=false,marker=false}:WatchRowProps){
 const {matches,filters,state}=useProduct();const st=watchStatus(w,matches),m=st.match;
 if(variant==='card')return <View style={s.card}>
  <View style={s.between}><T style={{fontFamily:'ManropeBold',fontSize:22}}>{w.symbol}</T><Badge label={st.badge} tone={st.tone}/></View>
  <T style={{fontSize:12,color:C.muted}}>{w.pattern_name} · {w.timeframe}</T>
  <T style={{fontSize:13}}>{st.detail}</T>
  <View style={s.row}>{m&&!!onReview&&<Button label={`Review ${w.symbol}`} kind="soft" onPress={()=>onReview(m)}/>}<Button label="Remove from watch" accessibilityLabel={`Remove ${w.symbol} ${w.pattern_name} ${w.timeframe} from watch`} kind="outline" onPress={onRemove}/></View>
 </View>;
 // Verdict comes from the same decision() as Discover; a setup missing from the latest scan has no verdict and shows the watch status instead.
 const d=m?decision(m,filters):null,chip=d?d.title.toUpperCase():st.badge,tone=d?(d.verdict==='review'?'green':d.verdict==='pass'?'red':'amber'):st.tone;
 const age=dataAgeDays(m?.candle_end||w.candle_end),stale=isStale(age,state?.source_stale),ageText=dataAgeText(age);
 const a11y=`Open ${w.symbol} ${w.pattern_name} ${w.timeframe} on the chart. ${d?d.title:'Pattern absent from the latest stored scan'}. ${ageText}${stale?', stale':''}.${st.badge==='NEW SNAPSHOT'?' New stored snapshot.':''}${selected?' Active selection.':marker?' Active symbol, selected elsewhere.':''}`;
 return <View style={[s.row,{gap:0,borderBottomWidth:1,borderColor:C.line}]}>
  <Pressable accessibilityRole="button" accessibilityLabel={a11y} accessibilityState={{selected}} disabled={!onSelect} onPress={()=>onSelect?.(w,m)} style={({pressed})=>({flex:1,minWidth:0,minHeight:48,justifyContent:'center',gap:2,paddingVertical:6,paddingLeft:9,paddingRight:4,borderLeftWidth:2,borderLeftColor:selected?C.green:marker?FAINT:'transparent',backgroundColor:selected?C.soft:pressed?C.paper:'transparent'})}>
   <View style={[s.row,{gap:6}]}><T numberOfLines={1} style={{fontFamily:'InterSemi',fontSize:13,flexShrink:1}}>{w.symbol}</T>{marker&&<View accessibilityElementsHidden importantForAccessibility="no" style={{width:6,height:6,borderRadius:3,backgroundColor:FAINT}}/>}<View style={{flex:1}}/><Badge label={chip} tone={tone}/></View>
   <T numberOfLines={1} style={{fontSize:11,lineHeight:16,color:C.muted}}>{w.pattern_name} · {w.timeframe} · <T style={{fontSize:11,color:stale?C.amber:C.muted}}>{ageText}{stale?' · stale':''}</T>{st.badge==='NEW SNAPSHOT'?' · new snapshot':''}</T>
  </Pressable>
  <IconButton icon="x" label={`Remove ${w.symbol} ${w.pattern_name} ${w.timeframe} from watch`} tooltip="Remove from watch" onPress={onRemove}/>
 </View>;
}
export const WATCH_WIDGET_KEY='watch',WATCH_WIDGET_TITLE='Watch',WATCH_WIDGET_SUBTITLE='My watch';
export type WatchWidgetProps={onOpenFull?:()=>void;onDiscover?:()=>void;framed?:boolean;limit?:number};
const openFullFn=(fn?:()=>void)=>fn||(()=>router.push('/watch' as any)),discoverFn=(fn?:()=>void)=>fn||(()=>router.push('/chart' as any));
/** Header ⋮ menu: Open full view · Discover setups. Fallbacks navigate to /watch and / when the shell passes no handler. */
export function watchWidgetMenu({onOpenFull,onDiscover}:{onOpenFull?:()=>void;onDiscover?:()=>void}={}):WidgetMenuItem[]{return [{label:'Open full view',icon:'maximize-2',onPress:openFullFn(onOpenFull)},{label:'Discover setups',icon:'search',onPress:discoverFn(onDiscover)}];}
/** Sidebar descriptor. Memoise the result in the shell: Sidebar re-renders a widget only when its descriptor object changes. */
export function watchWidget(opts:{onOpenFull?:()=>void;onDiscover?:()=>void;onClose?:()=>void;onMaximize?:()=>void}={}):SidebarWidget{return {key:WATCH_WIDGET_KEY,title:WATCH_WIDGET_TITLE,subtitle:WATCH_WIDGET_SUBTITLE,menu:watchWidgetMenu(opts),onClose:opts.onClose,onMaximize:opts.onMaximize,render:()=><WatchWidget onOpenFull={opts.onOpenFull} onDiscover={opts.onDiscover}/>};}
/** Body only by default (Sidebar supplies the Widget card + menu). framed wraps it in its own Widget card for use outside Sidebar. */
export function WatchWidget({onOpenFull,onDiscover,framed=false,limit=8}:WatchWidgetProps){
 const body=<WatchWidgetBody onOpenFull={onOpenFull} onDiscover={onDiscover} limit={limit}/>;
 return framed?<Widget title={WATCH_WIDGET_TITLE} subtitle={WATCH_WIDGET_SUBTITLE} menu={watchWidgetMenu({onOpenFull,onDiscover})}>{body}</Widget>:body;
}
function WatchWidgetBody({onOpenFull,onDiscover,limit}:{onOpenFull?:()=>void;onDiscover?:()=>void;limit:number}){
 const {product,matches,loading,error,refresh,state}=useProduct();const {active,setActive,isActive}=useActiveSymbol();const removal=useWatchRemoval();
 const watches:WatchItem[]=product?.watchlist||[],shown=watches.slice(0,limit);
 // Q1: the watched setup's matchId is sent only while it still exists in the stored scan. A PATTERN ABSENT row sets the symbol only; the chart then auto-selects within the Discover filters (labelled) or offers an "outside your filters" pick list, never a silent substitute.
 const select=(w:WatchItem,m?:Match)=>{setActive({symbol:w.symbol,timeframe:w.timeframe,matchId:m?w.id:undefined,source:'watch'})};
 let content:React.ReactNode;
 if(loading&&!watches.length)content=<T accessibilityLiveRegion="polite" style={{fontSize:12,color:C.muted}}>Loading your watch…</T>;
 else if(error&&!state)content=<WidgetError title="Could not load your watch" message={error} onRetry={refresh}/>;
 else if(!watches.length)content=<View style={{gap:8}}><T style={{fontFamily:'InterMedium',fontSize:13}}>Add a setup from Discover</T><T style={{fontSize:12,lineHeight:18,color:C.muted}}>Open a setup and choose Watch. Its pattern, timeframe and next conditions stay together here.</T><Button label="Discover setups" icon="arrow-right" kind="outline" onPress={discoverFn(onDiscover)} style={{minHeight:38,paddingVertical:6}}/></View>;
 else content=<View style={{gap:8}}>
  {!!error&&<Pressable accessibilityRole="button" accessibilityLabel="Watch refresh failed. Retry" onPress={refresh} style={{minHeight:32,justifyContent:'center'}}><T style={{fontSize:11,color:C.amber}}>Refresh failed · showing the last loaded watch · Retry</T></Pressable>}
  <View style={{borderWidth:1,borderColor:C.line,borderRadius:10,overflow:'hidden'}}>{shown.map(w=>{
   // Highlight follows the store (§10.2): strong when this exact watch setup was chosen from Watch; a faint marker when the active symbol is this row's but was chosen elsewhere (or is another setup of it).
   const m=watchStatus(w,matches).match,exact=!!active&&isActive(w.symbol,m?w.id:undefined)&&active.timeframe===w.timeframe&&(!!m||!active.matchId);
   const selected=exact&&active!.source==='watch',marker=!selected&&!!active&&active.symbol===String(w.symbol).toUpperCase();
   return <WatchRow key={w.id} item={w} variant="compact" selected={selected} marker={marker} onSelect={select} onRemove={()=>removal.setRemoving(w)}/>})}</View>
  {watches.length>limit&&<Button label={`Open full view · ${watches.length-limit} more`} kind="ghost" onPress={openFullFn(onOpenFull)} style={{minHeight:36,paddingVertical:4}}/>}
 </View>;
 return <>{content}{removal.sheet}</>;
}
