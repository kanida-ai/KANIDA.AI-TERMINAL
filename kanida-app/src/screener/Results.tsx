// RESULTS AND THE MATCH LIFECYCLE.
//
// A result is what matched and why — never buy, sell, good, bad or best. Each instrument is ONE card for the
// session, whatever happens to it: New match → Still matching → Strengthening / Weakening → Condition ended, and
// a later re-match is a new episode on the same card. Every sentence on a card was written by the server from
// the stored readings ("Matched because…"); nothing here computes a number.
import React,{useState} from 'react';
import {Pressable,View} from 'react-native';
import {Badge,C,Chip,Empty,Icon,Loading,T,s} from '../ui';
import {STATUS_LABEL,STATUS_TONE,TICK_GLYPH,expiryShort,nextReading} from './model';
import type {View as ResultView} from './api';
import type {Match,Results as ResultsBody,Tick} from './types';

const TICK_COLOR:Record<Tick,string>={none:C.line,unseen:'transparent',gap:C.muted,new:C.green,still:C.green,
 strengthening:C.green,weakening:C.amber,ended:C.muted};

function Timeline({m,readings}:{m:Match;readings:string[]}){
 return <View style={{gap:8}}>
  <View style={[s.row,{flexWrap:'wrap',gap:2}]} accessibilityLabel={`Timeline: ${m.timeline.map((t,i)=>`${readings[i]} ${t}`).join(', ')}`}>
   {m.timeline.map((t,i)=><View key={i} style={{alignItems:'center',width:30,gap:2}}>
    <T style={{fontSize:13,color:TICK_COLOR[t],lineHeight:16}}>{TICK_GLYPH[t]}</T>
    <T style={{fontSize:9,color:C.muted,lineHeight:12}}>{(readings[i]||'').replace(':','')}</T>
   </View>)}
  </View>
  <T style={{fontSize:10,color:C.muted}}>● new  ■ matching  ▲ strengthening  ▼ weakening  ○ ended  ⋯ reading not captured  · not matching</T>
  <T style={{fontSize:12,color:C.ink}}>
   First matched {m.first_matched} · {m.readings_matched} matching reading{m.readings_matched===1?'':'s'} today
   {m.episodes.length>1?` · ${m.episodes.length} separate episodes`:''} · {m.active?'still active':`ended ${m.ended_at}`}
  </T>
  {m.episodes.length>1&&<View style={{gap:2}}>{m.episodes.map((e,i)=><T key={i} style={{fontSize:11,color:C.muted}}>
   Episode {i+1}: {e.start} → {e.end||'now'} ({e.readings} reading{e.readings===1?'':'s'})</T>)}</View>}
 </View>;
}

function MatchCard({m,readings}:{m:Match;readings:string[]}){
 const [open,setOpen]=useState(false);
 const again=m.episodes.length>1&&m.status==='new';
 const title=m.strike!=null?`${m.underlying} · ${expiryShort(m.expiry)} · ${m.title.split(' · ')[1]}`:`${m.underlying} · ${expiryShort(m.expiry)} expiry`;
 return <View style={[s.card,{padding:16,gap:10,borderColor:m.active?C.line:C.line,opacity:m.active?1:.86}]}>
  <View style={[s.between,{alignItems:'flex-start',flexWrap:'wrap'}]}>
   <View style={{gap:6,flex:1,minWidth:220}}>
    <View style={[s.row,{gap:8,flexWrap:'wrap'}]}>
     <Badge label={m.status==='ended'?`ENDED ${m.ended_at}`:again?'MATCHED AGAIN':STATUS_LABEL[m.status]} tone={STATUS_TONE[m.status]} dot/>
     {m.computed&&<Badge label="IV COMPUTED" tone="amber"/>}
     {m.thin&&<Badge label="BELOW LIQUIDITY FLOORS" tone="neutral"/>}
    </View>
    <T style={{fontFamily:'ManropeBold',fontSize:17}}>{title}</T>
   </View>
   <T style={{fontSize:11,color:C.muted,textAlign:'right'}}>
    {again?`again ${m.episode_started} · first ${m.first_matched}`:`first ${m.first_matched}`}{'\n'}
    {m.episode_readings} reading{m.episode_readings===1?'':'s'}{m.duration_min?` · ${m.duration_min} min`:''}
   </T>
  </View>
  <View style={{gap:4}}>
   <T style={[s.label,{fontSize:10}]}>{m.active?'Matched because':`Matched ${m.episode_started}–${m.last_matched} because`}</T>
   {m.because.map((b,i)=><T key={i} style={{fontSize:13,lineHeight:20}}>{b}</T>)}
   {!!m.ended_because&&<T style={{fontSize:13,lineHeight:20,color:C.muted}}>Ended: {m.ended_because}</T>}
  </View>
  <Pressable accessibilityRole="button" accessibilityState={{expanded:open}} accessibilityLabel={open?'Hide timeline':'Show timeline'}
   onPress={()=>setOpen(o=>!o)} style={[s.row,{gap:6,alignSelf:'flex-start',paddingVertical:4}]}>
   <Icon name={open?'chevron-up':'clock'} size={13} color={C.green}/>
   <T style={{fontSize:12,color:C.green,fontFamily:'InterMedium'}}>{open?'Hide today’s timeline':'Today’s timeline'}</T>
  </Pressable>
  {open&&<Timeline m={m} readings={readings}/>}
 </View>;
}

export function ResultsPanel({body,loading,error,view,onView,title}:{body:ResultsBody|null;loading:boolean;error:string;
 view:ResultView;onView:(v:ResultView)=>void;title:string}){
 const counts=body?.counts;
 const next=nextReading(body?.as_of);
 return <View style={{gap:14}}>
  <View style={[s.between,{flexWrap:'wrap'}]}>
   <View style={{gap:3,flex:1,minWidth:220}}>
    <T style={{fontFamily:'ManropeBold',fontSize:19}}>{title}</T>
    {!!body?.as_of&&<T style={{fontSize:12,color:C.muted}}>As of the {body.as_of.slice(11,16)} reading, {body.session}
     {next?` · next reading ~${next}`:' · session closed'}{body.elapsed_ms!=null?` · evaluated in ${(body.elapsed_ms/1000).toFixed(1)}s`:''}</T>}
   </View>
   <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
    <Chip label={`Active ${counts?counts.active:'–'}`} active={view==='active'} onPress={()=>onView('active')}/>
    <Chip label={`Ended today ${counts?counts.ended:'–'}`} active={view==='ended'} onPress={()=>onView('ended')}/>
    <Chip label={`All ${counts?counts.all:'–'}`} active={view==='all'} onPress={()=>onView('all')}/>
   </View>
  </View>
  {(body?.notes||[]).map((n,i)=><View key={i} style={[s.row,{gap:8,alignItems:'flex-start',padding:12,borderRadius:10,backgroundColor:C.amberBg}]}>
   <Icon name="info" size={14} color={C.amber}/><T style={{flex:1,fontSize:12,lineHeight:18,color:C.amber}}>{n.text}</T></View>)}
  {loading&&!body?<Loading/>:error?<Empty icon="alert-circle" title="Could not run this scanner" detail={error}/>
   :!body?null:!body.available?<Empty icon="clock" title="No readings yet" detail={body.text||''}/>
   :!body.matches.length?<Empty icon="search" title={view==='ended'?'Nothing has ended today':'Nothing matches right now'}
     detail={view==='active'&&counts&&counts.ended?`${counts.ended} instrument${counts.ended===1?'':'s'} matched earlier today and ended — see "Ended today".`
      :'KANIDA checks again at every 15-minute reading.'}/>
   :<View style={{gap:10}}>
     {loading&&<T style={{fontSize:12,color:C.muted}}>Refreshing…</T>}
     {body.matches.map(m=><MatchCard key={m.key} m={m} readings={body.readings}/>)}
     {body.truncated&&<T style={{fontSize:12,color:C.muted}}>Showing the first {body.shown} — the newest changes first.</T>}
    </View>}
 </View>;
}
