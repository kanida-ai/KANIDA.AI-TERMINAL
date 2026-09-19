import React,{useCallback,useEffect,useRef,useState} from 'react';
import {View,Pressable,TextInput} from 'react-native';
import {router,usePathname} from 'expo-router';
import {useProduct} from '../context';
import {C,T,s,Icon} from '../ui';
import {api,Match} from '../model';
import {TIMEFRAMES} from '../constants';
import {useActiveSymbol,DEFAULT_TIMEFRAME} from '../activeSymbol';
import {Popover,useRoving} from '../layout/index';
import {webOnly} from '../layout/shared';
import {isWorkspacePath,CHART_PATH} from './routes';
// Off the workspace (Falcon, Discover Strategies, admin…) a pick also opens the full chart at /chart, which follows the store.
// Top-bar symbol search (§10.1/§10.2). Queries the existing /api/stocks search (the same call Discover and Simulate use) and lists results in a Popover (Escape, outside press and route change close it).
// Choosing a result writes the active-symbol store with source 'search': symbol + the current timeframe, else the first timeframe with a stored setup for that stock, else 1D. No match id is invented: the chart resolves the stored setup itself (Q1: only within the Discover filters, written back and labelled "Auto-selected"; otherwise an "outside your filters" pick list).
type Hit={symbol:string;company?:string;sector?:string};
export function SymbolSearch({compact=false,onPicked}:{compact?:boolean;onPicked?:()=>void}){
 const p=useProduct(),{active,setActive}=useActiveSymbol(),routeKey=usePathname();
 const [open,setOpen]=useState(false),[q,setQ]=useState(''),[hits,setHits]=useState<Hit[]>([]),[loading,setLoading]=useState(false),[error,setError]=useState('');
 const trigger=useRef<any>(null),roving=useRoving(hits.length,'vertical');
 useEffect(()=>{const text=q.trim();setHits([]);setError('');setLoading(false);if(!open||text.length<2)return;let live=true;setLoading(true);const timer=setTimeout(()=>api('/api/stocks?q='+encodeURIComponent(text)).then((v:any)=>{if(live){setHits(Array.isArray(v)?v.slice(0,8):[]);setLoading(false)}}).catch((e:any)=>{if(live){setError(e?.message||'Search is unavailable');setLoading(false)}}),240);return()=>{live=false;clearTimeout(timer)}},[q,open]);
 const close=useCallback(()=>setOpen(false),[]);
 const matches:Match[]=p.matches||[];
 const frames=(symbol:string)=>{const sym=String(symbol||'').toUpperCase(),own=matches.filter(m=>m.symbol===sym);return [...TIMEFRAMES.filter(tf=>own.some(m=>m.timeframe===tf)),...Array.from(new Set(own.map(m=>m.timeframe))).filter(tf=>!TIMEFRAMES.includes(tf))];};
 function pick(h?:Hit){if(!h)return;const symbol=String(h.symbol||'').toUpperCase(),timeframe=active?.timeframe||frames(symbol)[0]||DEFAULT_TIMEFRAME;if(setActive({symbol,timeframe,source:'search'})){setOpen(false);setQ('');onPicked?.();if(!isWorkspacePath(routeKey))router.push(CHART_PATH as any)}else p.setToast(`${symbol} can’t be opened on the chart.`)}
 const text=q.trim(),status=text.length<2?'Type at least 2 characters of a symbol or company.':loading?'Searching…':error?`Search failed: ${error}`:!hits.length?'No stocks found.':`${hits.length} ${hits.length===1?'stock':'stocks'} · Enter opens the first · ↓ moves into the list`;
 return <>
  <Pressable ref={trigger} accessibilityRole="button" accessibilityLabel={active?`Search stocks. Active stock ${active.symbol}, ${active.timeframe}`:'Search stocks'} {...webOnly({'aria-haspopup':'dialog','aria-expanded':open})} onPress={()=>setOpen(true)} style={(st:any)=>[s.row,{flex:1,minWidth:0,maxWidth:compact?undefined:440,minHeight:38,gap:8,paddingHorizontal:12,borderRadius:10,borderWidth:1,borderColor:open||st.focused?C.green:C.line,backgroundColor:st.hovered?C.soft:C.paper}]}>
   <Icon name="search" size={15} color={C.muted}/>
   <T numberOfLines={1} style={{flex:1,fontSize:13,color:active?C.ink:C.muted}}>{active?<>{active.symbol}<T style={{fontSize:12,color:C.muted}}> · {active.timeframe} · Search stocks</T></>:'Search stocks'}</T>
  </Pressable>
  <Popover open={open} onClose={close} anchor={trigger} placement="bottom-start" label="Search stocks" width={compact?340:420} routeKey={routeKey}>
   <View style={{paddingHorizontal:10,paddingTop:4,paddingBottom:6,gap:6}}>
    <TextInput autoFocus accessibilityLabel="Stock symbol or company" value={q} onChangeText={setQ} onSubmitEditing={()=>pick(hits[0])} onKeyPress={(e:any)=>{if(e?.nativeEvent?.key==='ArrowDown'&&hits.length){e.preventDefault?.();roving.refs.current[0]?.focus?.()}}} placeholder="Symbol or company" placeholderTextColor={C.muted} autoCapitalize="characters" autoCorrect={false} returnKeyType="search" style={[s.input,{minHeight:42,fontSize:14}]}/>
    <T accessibilityLiveRegion="polite" style={{fontSize:11,lineHeight:16,color:error?C.amber:C.muted}}>{status}</T>
   </View>
   {hits.length>0&&<View accessibilityLabel="Stock results" style={{borderTopWidth:1,borderColor:C.line}}>{hits.map((h,i)=>{const tf=frames(h.symbol);return <Pressable key={h.symbol+i} ref={(el:any)=>{roving.refs.current[i]=el;}} accessibilityRole="button" accessibilityLabel={`Open ${h.symbol}${h.company?`, ${h.company}`:''} on the chart. ${tf.length?`Stored setups on ${tf.join(', ')}`:'No stored setup'}`} {...webOnly({onKeyDown:(e:any)=>roving.onKeyDown(i,e)})} onPress={()=>pick(h)} style={(st:any)=>[s.between,{minHeight:44,paddingHorizontal:14,paddingVertical:6,gap:8,backgroundColor:st.hovered||st.focused||st.pressed?C.soft:'transparent'}]}>
    <View style={{flex:1,minWidth:0}}><T style={{fontFamily:'InterSemi',fontSize:13}}>{h.symbol}</T>{!!h.company&&<T numberOfLines={1} style={{fontSize:11,lineHeight:16,color:C.muted}}>{h.company}</T>}</View>
    <T style={{fontSize:11,color:tf.length?C.green:C.muted}}>{tf.length?tf.join(' · '):'No stored setup'}</T>
   </Pressable>})}</View>}
  </Popover>
 </>;
}
