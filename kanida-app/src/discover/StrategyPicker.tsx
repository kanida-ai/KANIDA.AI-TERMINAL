// Strategy picker (spec §3.4): Popover anchored under the card title, ~480 px, max 70% height. Search focused, registry-tag chips, recent first, keyboard ↓ from search, ↑/↓, Enter, Esc.
import React,{useEffect,useMemo,useRef,useState} from 'react';
import {View,Pressable,ScrollView,TextInput,useWindowDimensions} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {Popover,IconButton,readStore,writeStore,type PopoverAnchor} from '../layout';
import type {StrategySummary} from '../strategies/types';
import {filterStrategies,pickerChips,pickerTag,pushRecent} from './logic';
import {researchPickerTag} from './cardLogic';
// Researched strategies have no live detector yet, so their tag reports the researched history, never "N found".
const tagFor=(st:any)=>st?.source_type==='research_pattern'?researchPickerTag(st):pickerTag(st);
// Dim a row only when there is genuinely nothing behind it; "evidence still indexing" is not "nothing".
const dimmed=(st:any)=>st?.source_type==='research_pattern'?!st.evidence_pending&&!st.researched_stocks:!st.found;
import {web,webOnly} from './parts';
export function readRecent():string[]{const v=readStore<{keys:string[]}>('discover','recent').keys;return Array.isArray(v)?v.filter(k=>typeof k==='string'):[];}
export function StrategyPicker({open,onClose,anchor,strategies,currentKey,universeLabel,onChoose,cardLabel}:{open:boolean;onClose:()=>void;anchor:PopoverAnchor;strategies:StrategySummary[];currentKey:string|null;universeLabel:string;onChoose:(key:string)=>void;cardLabel:string}){
 const {height:H,width:W}=useWindowDimensions(),phone=W<760;
 const [query,setQuery]=useState(''),[chip,setChip]=useState('All'),[recent,setRecent]=useState<string[]>([]);
 const input=useRef<any>(null),rows=useRef<any[]>([]);
 useEffect(()=>{if(!open)return;setQuery('');setChip('All');setRecent(readRecent());const t=setTimeout(()=>input.current?.focus?.(),30);return ()=>clearTimeout(t)},[open]);
 const ordered=useMemo(()=>[...strategies].sort((a,b)=>a.order-b.order),[strategies]);
 const chips=useMemo(()=>pickerChips(ordered),[ordered]);
 const sections=useMemo(()=>{
  if(chip==='All'&&!query.trim()){const rec=recent.map(k=>ordered.find(x=>x.key===k)).filter(Boolean) as StrategySummary[];return [...(rec.length?[{title:'Recently used',items:rec}]:[]),{title:'All strategies',items:ordered}]}
  if(chip==='Recently used')return [{title:'Recently used',items:filterStrategies(ordered,query,chip,recent)}];
  return [{title:chip==='All'?'Matching strategies':chip,items:filterStrategies(ordered,query,chip,recent)}];
 },[ordered,chip,query,recent]);
 const flat=sections.flatMap(x=>x.items);rows.current.length=flat.length;
 const choose=(key:string)=>{writeStore('discover','recent',{keys:pushRecent(readRecent(),key)});onClose();onChoose(key)};
 const focusRow=(i:number)=>{if(i<0){input.current?.focus?.();return}rows.current[Math.min(i,flat.length-1)]?.focus?.()};
 const rowKey=(i:number,e:any)=>{const k=e?.key;let j=-2;if(k==='ArrowDown')j=Math.min(flat.length-1,i+1);else if(k==='ArrowUp')j=i-1;else if(k==='Home')j=0;else if(k==='End')j=flat.length-1;if(j<-1)return;e.preventDefault?.();focusRow(j)};
 let index=-1;
 return <Popover open={open} onClose={onClose} anchor={anchor} placement="bottom-start" label={`Choose a strategy for ${cardLabel}`} width={480}>
  <View style={{maxHeight:Math.round(H*.7),minHeight:0,flexShrink:1}}>
   <View style={[s.between,{paddingLeft:14,paddingRight:4}]}><T style={s.label}>Choose a strategy</T><IconButton icon="x" label="Close strategy picker" tooltip="Close" onPress={onClose}/></View>
   <View style={{paddingHorizontal:12,gap:8,paddingBottom:8}}>
    <View style={[s.row,{gap:8,borderWidth:1,borderColor:C.line,borderRadius:10,paddingHorizontal:10,backgroundColor:C.bg}]}><Icon name="search" size={14} color={C.muted}/>
     <TextInput ref={input} autoFocus accessibilityLabel="Search strategies" value={query} onChangeText={setQuery} placeholder="Search pattern, side or timeframe…" placeholderTextColor={C.muted} onSubmitEditing={()=>{if(flat[0])choose(flat[0].key)}} onKeyPress={(e:any)=>{if(e?.nativeEvent?.key==='ArrowDown'&&flat.length){e.preventDefault?.();focusRow(0)}}} style={[{flex:1,minWidth:0,height:36,color:C.ink,fontFamily:'Inter',fontSize:13},web?({outlineStyle:'none'} as any):null]}/></View>
    <View role="group" aria-label="Filter strategies" style={[s.row,{flexWrap:'wrap',gap:6}]}>{chips.map(c=>{const on=c===chip;return <Pressable key={c} accessibilityRole="button" accessibilityLabel={`Filter: ${c}`} accessibilityState={{selected:on}} {...webOnly({'aria-pressed':on})} onPress={()=>setChip(c)} style={(st:any)=>({minHeight:28,justifyContent:'center',paddingHorizontal:10,borderRadius:14,borderWidth:1,borderColor:on?C.green:C.line,backgroundColor:on?C.soft:st.hovered||st.focused?C.bg:'transparent'})}><T style={{fontSize:11,lineHeight:15,fontFamily:'InterMedium',color:on?C.green:C.muted}}>{c}</T></Pressable>})}</View>
   </View>
   <ScrollView style={{flexShrink:1,minHeight:0,borderTopWidth:1,borderColor:C.line}} contentContainerStyle={{paddingVertical:4}} keyboardShouldPersistTaps="handled">
    {sections.map(sec=><View key={sec.title} role="group" aria-label={sec.title}>
     <T style={[s.label,{fontSize:10,paddingHorizontal:14,paddingTop:8,paddingBottom:4}]}>{sec.title}</T>
     {!sec.items.length&&<T style={{fontSize:12,color:C.muted,paddingHorizontal:14,paddingVertical:8}}>{sec.title==='Recently used'&&!query?'Nothing used yet.':'No strategy matches this search.'}</T>}
     {sec.items.map(st=>{const i=++index,on=st.key===currentKey,dim=dimmed(st);return <Pressable key={sec.title+st.key} ref={(el:any)=>{rows.current[i]=el}} accessibilityRole="button" accessibilityLabel={`${st.name}. ${universeLabel} · ${tagFor(st)}${on?'. Current':''}`} accessibilityState={{selected:on}} {...webOnly({'aria-current':on?'true':undefined,onKeyDown:(e:any)=>rowKey(i,e)})} onPress={()=>choose(st.key)} style={(st2:any)=>[s.row,{alignItems:'flex-start',gap:10,paddingHorizontal:14,paddingVertical:8,backgroundColor:st2.hovered||st2.focused?C.soft:'transparent'}]}>
      <View style={{flex:1,minWidth:0,gap:2,opacity:dim?.55:1}}><View style={[s.row,{gap:6}]}>{on&&<Icon name="check" size={13} color={C.green}/>}<T numberOfLines={phone?2:1} style={{fontSize:13,lineHeight:18,fontFamily:'InterSemi',color:on?C.green:C.ink,flexShrink:1}}>{st.name}</T></View><T numberOfLines={1} style={{fontSize:11,lineHeight:15,color:C.muted}}>{(st.tags||[]).join(' · ')}</T></View>
      <View style={{maxWidth:'48%',flexShrink:0,borderRadius:6,paddingHorizontal:7,paddingVertical:3,backgroundColor:dim?C.bg:C.soft,opacity:dim?.7:1}}><T numberOfLines={1} style={{fontSize:10,lineHeight:14,fontFamily:'InterMedium',color:dim?C.muted:C.green,textAlign:'right',fontVariant:['tabular-nums']}}>{tagFor(st)}</T></View>
     </Pressable>})}
    </View>)}
   </ScrollView>
  </View>
 </Popover>;
}
