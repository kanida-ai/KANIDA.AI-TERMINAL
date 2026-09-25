// K03 template chooser (GTM audit P17): search + filters by direction, risk, complexity and leg count, a concise
// defined-risk default, advanced structures one tap away. Used by the builder (replace legs) and the start flow
// (no draft exists until a template is picked).
import React,{useEffect,useMemo,useState} from 'react';
import {Pressable,TextInput,View} from 'react-native';
import {Badge,Button,C,Chip,Sheet,T,s} from '../ui';
import {sb,type Template} from './api';
import {Sketch,TemplateIntro} from './Learn';
import {LoadState} from './States';

const GROUPS=[['bullish','Bullish'],['bearish','Bearish'],['range','Range'],['volatility','Big move']] as const;
const SIZES=[['all','Any legs'],['1-2','1-2 legs'],['3-4','3-4 legs'],['5+','5+ legs']] as const;

export function TemplateSheet({visible,onClose,onPick,replacing,title='Choose a template',header}:{visible:boolean;onClose:()=>void;
 onPick:(t:Template,p:number|null)=>void;replacing:number;title?:string;header?:React.ReactNode}){
 const [data,setData]=useState<{templates:Template[];later:any[];legging?:string}|null>(null);const [err,setErr]=useState('');
 const [adv,setAdv]=useState(false);const [param,setParam]=useState<Record<string,number>>({});const [intro,setIntro]=useState<Template|null>(null);
 const [q,setQ]=useState('');const [dir,setDir]=useState<string>('all');const [size,setSize]=useState<string>('all');
 const load=()=>{setErr('');sb.templates().then(setData).catch(e=>setErr(e?.message||'Templates could not be loaded.'));};
 useEffect(()=>{if(visible&&!data)load();},[visible,data]);// eslint-disable-line react-hooks/exhaustive-deps
 const fits=(t:Template)=>{
  if(!adv&&t.risk!=='defined')return false;
  if(dir!=='all'&&t.intent!==dir)return false;
  if(size==='1-2'&&t.legs>2)return false;if(size==='3-4'&&(t.legs<3||t.legs>4))return false;if(size==='5+'&&t.legs<5)return false;
  const x=q.trim().toLowerCase();return !x||`${t.name} ${t.recipe} ${t.use}`.toLowerCase().includes(x);};
 const shown=useMemo(()=>data?data.templates.filter(fits):[],[data,adv,dir,size,q]);// eslint-disable-line react-hooks/exhaustive-deps
 return <Sheet visible={visible} onClose={onClose} wide title={title}
  subtitle={replacing?`Using a template replaces the ${replacing} current leg${replacing>1?'s':''}. You will see the change first, and can undo it.`:'A template is a recipe; it becomes exact contracts from the current chain, all editable.'}>
  {header}
  <LoadState loading={!data&&!err} error={err} onRetry={load}>
   {data&&<>
    <View style={{gap:8}}>
     <TextInput value={q} onChangeText={setQ} placeholder="Search templates (e.g. condor, spread, hedge)" placeholderTextColor={C.muted} accessibilityLabel="Search templates"
      style={{height:44,borderWidth:1,borderColor:C.line,borderRadius:10,paddingHorizontal:12,color:C.ink,fontFamily:'Inter',fontSize:13,backgroundColor:C.paper}}/>
     <View style={[s.row,{flexWrap:'wrap',gap:6}]}>{[['all','All directions'],...GROUPS].map(([k,l])=><Chip key={k} label={l} active={dir===k} onPress={()=>setDir(k)}/>)}</View>
     <View style={[s.row,{flexWrap:'wrap',gap:6}]}>{SIZES.map(([k,l])=><Chip key={k} label={l} active={size===k} onPress={()=>setSize(k)}/>)}
      <Chip label={adv?'Defined risk + unhedged':'Defined risk only'} active={adv} onPress={()=>setAdv(!adv)}/></View>
     <T style={{fontSize:11,color:C.muted}} accessibilityLiveRegion="polite">{`${shown.length} of ${data.templates.length} templates`}</T>
    </View>
    {!shown.length&&<View style={[s.row,{gap:10}]}><T style={{color:C.muted,fontSize:12}}>No template matches these filters.</T>
     <Button label="Clear filters" kind="outline" onPress={()=>{setQ('');setDir('all');setSize('all');}}/></View>}
    {GROUPS.map(([k,title])=>{const list=shown.filter(t=>t.intent===k);if(!list.length)return null;
     return <View key={k} style={{gap:8}}><T style={label}>{title}</T>{list.map(t=><View key={t.key} style={[panel,{gap:6}]}>
      <View style={[s.between,{gap:8}]}><View style={[s.row,{gap:10,flex:1}]}><Sketch pts={t.sketch}/><T style={{fontFamily:'InterSemi',flexShrink:1}}>{t.name}</T></View>
       <Badge label={t.risk==='defined'?'Defined risk':'UNHEDGED'} tone={t.risk==='defined'?'green':'red'}/></View>
      <T style={{fontSize:11,color:C.muted}}>{`${t.legs} leg${t.legs>1?'s':''} · complexity ${t.complexity}/3`}</T>
      <T style={{fontSize:12,color:C.muted}}>{t.recipe}</T><T style={{fontSize:12}}>{`Use: ${t.use}`}</T><T style={{fontSize:12,color:C.amber}}>{`Loses when: ${t.loses}`}</T>
      <View style={[s.row,{flexWrap:'wrap',gap:6}]}>
       {t.param&&<><T style={{fontSize:11,color:C.muted}}>{t.param.label}</T>{t.param.variants.map(v=><Chip key={v} label={String(v)} active={(param[t.key]??t.param!.default)===v} onPress={()=>setParam(p=>({...p,[t.key]:v}))}/>)}</>}
       <View style={{flex:1}}/><Button label="Use" icon="arrow-right" accessibilityLabel={`Use ${t.name}`} onPress={()=>t.intro_required?setIntro(t):onPick(t,t.param?(param[t.key]??t.param.default):null)}/></View>
     </View>)}</View>;})}
    <Pressable accessibilityRole="button" onPress={()=>setAdv(!adv)}><T style={{fontSize:12,color:C.green}}>{adv?'Hide unhedged structures':'Show unhedged structures (short options - large or unlimited loss)'}</T></Pressable>
    <View style={{gap:4}}><T style={label}>Not in this release</T>{data.later.map(l=><T key={l.key} style={{fontSize:12,color:C.muted}}>{`${l.name} - ${l.reason}`}</T>)}</View>
    <TemplateIntro t={intro} legging={data.legging||''} onCancel={()=>setIntro(null)} onConfirm={()=>{const t=intro!;setIntro(null);onPick(t,t.param?(param[t.key]??t.param.default):null);}}/>
   </>}
  </LoadState>
 </Sheet>;
}
const panel={backgroundColor:C.paper,borderWidth:1,borderColor:C.line,borderRadius:12,padding:14,gap:12};
const label={fontFamily:'InterMedium',fontSize:10,letterSpacing:.6,textTransform:'uppercase' as const,color:C.muted};
