// Scanner card (spec §3.3): title = strategy ▾ (picker), ⋮ menu, ✕ clear, status line (ⓘ · ↻ · text · Stop/Retry), staggered row reveal, selection + ↑/↓/Enter.
import React,{useEffect,useMemo,useRef,useState} from 'react';
import {View,Pressable,ScrollView,Animated,Easing,AccessibilityInfo} from 'react-native';
import {C,T,Icon,s} from '../ui';
import {pct} from '../model';
import {Popover,MenuList,IconButton,readStore,writeStore,formatDataDate,type MenuItem} from '../layout';
import type {LiveDetection,StrategyRow,StrategySummary} from '../strategies/types';
import {useLiveDetections,useStrategyResults} from './useStrategies';
import {LiveList} from './LiveList';
import {sortRows,isSortKey,SORT_LABELS,statusText,rowSampleNote,revealStep,type SortKey} from './logic';
import {RESEARCH_COLUMNS,STORED_COLUMNS,ROW_PENDING,evidenceCoverageText,researchStatusText,rowPending,plainEvidence,
 LIST_MODE_LABELS,latestSession,listModeLabel,liveAvailable,liveOffText,liveStatusText,resolveListMode,type ListMode} from './cardLogic';
import {sameRow,type BlockLink,type Slot} from './link';
import {StrategyPicker} from './StrategyPicker';
import {CardShell,CenterNote,StaleLine,web,webOnly} from './parts';
export type ScannerCardProps={blockKey:string;slot:Slot;strategy:StrategySummary|null;strategies:StrategySummary[];universeKey:string;universeLabel:string;link:BlockLink|null;refreshSeq:number;resetSeq:number;
 autoSelect?:{symbol:string;sig:string}|null;onChoose:(key:string|null)=>void;onSelect:(row:StrategyRow,strategy:StrategySummary)=>void;onOpenFull:(row:StrategyRow,strategy:StrategySummary)=>void;
 /** Selecting a LIVE detection carries the detection itself, so the chart draws THAT episode and the evidence
  * card is gated on THAT detection's identity - never on the pattern name. */
 onSelectDetection?:(d:LiveDetection,strategy:StrategySummary)=>void;onOpenDetection?:(d:LiveDetection,strategy:StrategySummary)=>void;
 selectedDetectionId?:string|null;
 /** The block's own reason for having nothing to list (e.g. the scanner is on the researched set). Shown
  * instead of the generic "results unavailable" error, which reads like a broken app when it is not. */
 unavailable?:string|null;style?:any};
const tone=(v:number|null|undefined)=>v==null?C.muted:v>0?C.green:v<0?C.red:C.ink;
const head={fontSize:10,lineHeight:14,fontFamily:'InterMedium',letterSpacing:.6,textTransform:'uppercase' as const,color:C.muted};
const SORTS:SortKey[]=['low','avg','n','symbol'];
const COL={low:52,avg:48,n:28},tab={fontVariant:['tabular-nums'] as any};  // numeric columns: fixed, right-aligned, tabular
export function ScannerCard({blockKey,slot,strategy,strategies,universeKey,universeLabel,link,refreshSeq,resetSeq,autoSelect,onChoose,onSelect,onOpenFull,onSelectDetection,onOpenDetection,selectedDetectionId,unavailable,style}:ScannerCardProps){
 const storeId=`card.${blockKey}.${slot}`,skey=strategy?.key??null;
 // Researched patterns and the stored scan put DIFFERENT statistics in the same three columns, so the headings
 // and the row's spoken label follow the source. A number is never shown under a heading that misdescribes it.
 const research=(strategy as any)?.source_type==='research_pattern',cols=research?RESEARCH_COLUMNS:STORED_COLUMNS;
 const [sort,setSort]=useState<SortKey>(()=>{const v=readStore<{sort:string}>('discover',storeId).sort;return isSortKey(v)?v:'low'});
 const chooseSort=(k:SortKey)=>{setSort(k);writeStore('discover',storeId,{sort:k,mode})};
 const firstReset=useRef(resetSeq);useEffect(()=>{if(resetSeq===firstReset.current)return;firstReset.current=resetSeq;setSort('low');writeStore('discover',storeId,{})},[resetSeq]);
 const res=useStrategyResults(skey,universeKey,refreshSeq);
 // "Now" (setups standing right now) and "History" (where the pattern happened before) are two different lists
 // of two different things, so the card shows one at a time and names which. "Now" is only offered when the
 // scanner's record is actually readable; otherwise the card behaves exactly as it did before.
 // Until this card's own results land, the catalog summary already says whether live detection is on, so the
 // card opens on the right list instead of showing a frame of the history list and then swapping.
 const liveState=(res.data as any)?.live??null;
 const canLive=research&&(res.data?liveAvailable(liveState):(strategy as any)?.live_detection===true);
 const [mode,setMode]=useState<ListMode>(()=>(readStore<{mode:string}>('discover',storeId).mode==='history'?'history':'live'));
 const listMode:ListMode=canLive?resolveListMode(mode,liveState):'history';
 const chooseMode=(m:ListMode)=>{setMode(m);writeStore('discover',storeId,{sort,mode:m})};
 // The Now tab is every setup still forming or confirmed, whenever it was first found: a Monday wedge that is
 // still valid is exactly what a trader needs to see. There is no narrower "today" filter - a setup first found
 // on the latest session carries a small "new" marker on its own row instead.
 const det=useLiveDetections(skey,'live',refreshSeq,canLive&&listMode==='live');
 const detRows=det.data?.rows||[],detTotal=det.data?.total??0;
 const session=latestSession(det.data?.live||liveState,strategy?.timeframe);
 const [reduce,setReduce]=useState(false);useEffect(()=>{AccessibilityInfo.isReduceMotionEnabled().then(setReduce).catch(()=>{})},[]);
 // Staggered reveal: one step per 25 ms. Stop freezes it (the found count stays the true total); Show all finishes it.
 const [reveal,setReveal]=useState<{data:any;shown:number;stopped:boolean}>({data:null,shown:0,stopped:false});
 useEffect(()=>{const d=res.data;if(!d){setReveal({data:null,shown:0,stopped:false});return}const total=d.rows.length;if(reduce||!total){setReveal({data:d,shown:total,stopped:false});return}
  setReveal({data:d,shown:0,stopped:false});const step=revealStep(total);let shown=0;const t=setInterval(()=>{shown=Math.min(total,shown+step);setReveal(r=>r.data!==d||r.stopped?r:{...r,shown});if(shown>=total)clearInterval(t)},25);return ()=>clearInterval(t)},[res.data,reduce]);
 const rows=useMemo(()=>sortRows(res.data?.rows||[],sort),[res.data,sort]);
 const shown=reveal.data===res.data?reveal.shown:0,visible=rows.slice(0,shown),total=res.data?(res.data.total??rows.length):0;
 const inFlight=res.phase==='loading',revealing=res.phase==='done'&&shown<rows.length&&!reveal.stopped,cut=res.phase==='done'&&shown<rows.length&&reveal.stopped;
 // Progress, Stop and Retry follow whichever list is on screen; declared here so the progress animation can
 // depend on it.
 const busy=listMode==='live'?det.phase==='loading':inFlight;
 const failed=listMode==='live'?det.phase==='error'||det.phase==='stopped':res.phase==='error'||res.phase==='stopped';
 const selIndex=visible.findIndex(r=>sameRow(link,slot,skey,r));
 const date=formatDataDate(res.data?.data_end||undefined)||'unknown date',uni=res.data?.universe?.label||universeLabel;
 // Deep link: select the requested stock once per link signature when it is in this strategy's results.
 const [missing,setMissing]=useState('');const consumed=useRef('');
 useEffect(()=>setMissing(''),[skey]);
 const rowRefs=useRef<any[]>([]);
 // A deep link names a stock; it is resolved in the list the card is SHOWING, because that is the list the
 // user will see it selected in. On the live list that means today's detection for that stock.
 useEffect(()=>{if(!autoSelect||!strategy||listMode!=='live'||det.phase!=='done'||!det.data)return;
  const id=autoSelect.sig+'|'+strategy.key+'|live';if(consumed.current===id)return;consumed.current=id;
  const hit=detRows.find(r=>r.symbol===autoSelect.symbol);
  if(!hit){setMissing(autoSelect.symbol);return}
  setMissing('');onSelectDetection?.(hit,strategy);
 },[autoSelect?.sig,strategy?.key,listMode,det.phase,det.data,detRows]);
 useEffect(()=>{if(!autoSelect||!strategy||listMode==='live'||res.phase!=='done'||!res.data)return;const id=autoSelect.sig+'|'+strategy.key;if(consumed.current===id)return;consumed.current=id;
  const i=rows.findIndex(r=>r.symbol===autoSelect.symbol);if(i<0){setMissing(autoSelect.symbol);return}
  setMissing('');setReveal({data:res.data,shown:rows.length,stopped:false});onSelect(rows[i],strategy);setTimeout(()=>rowRefs.current[i]?.scrollIntoView?.({block:'nearest'}),80);
 },[autoSelect?.sig,strategy?.key,listMode,res.phase,res.data,rows]);
 const slide=useRef(new Animated.Value(0)).current;
 useEffect(()=>{if(!busy||reduce)return;slide.setValue(0);const a=Animated.loop(Animated.timing(slide,{toValue:1,duration:1100,easing:Easing.inOut(Easing.ease),useNativeDriver:false}));a.start();return ()=>a.stop()},[busy,reduce]);
 const [pickerOpen,setPickerOpen]=useState(false),[menuOpen,setMenuOpen]=useState(false),[infoOpen,setInfoOpen]=useState(false);
 const titleRef=useRef<any>(null),ctaRef=useRef<any>(null),menuRef=useRef<any>(null),infoRef=useRef<any>(null);const [anchor,setAnchor]=useState<React.RefObject<any>>(titleRef);
 const openPicker=(ref:React.RefObject<any>)=>{setAnchor(ref);setPickerOpen(true)};
 const selectAt=(j:number)=>{const row=visible[j];if(!row||!strategy)return;onSelect(row,strategy);setTimeout(()=>{const el=rowRefs.current[j];el?.focus?.();el?.scrollIntoView?.({block:'nearest'})},0)};
 const rowKey=(i:number,e:any)=>{const k=e?.key;if(k==='ArrowDown'||k==='ArrowUp'||k==='Home'||k==='End'){e.preventDefault?.();selectAt(k==='Home'?0:k==='End'?visible.length-1:Math.max(0,Math.min(visible.length-1,i+(k==='ArrowDown'?1:-1))))}else if(k==='Enter'&&strategy&&visible[i]){e.preventDefault?.();onOpenFull(visible[i],strategy)}};
 const menu:MenuItem[]=[
  {label:'Refresh',icon:'refresh-cw',onPress:res.reload,disabled:!strategy},
  ...SORTS.map(k=>({label:`Sort by ${k==='symbol'?SORT_LABELS[k]:cols[k]}${sort===k?' (current)':''}`,icon:sort===k?'check':'minus',onPress:()=>chooseSort(k),disabled:!strategy})),
  {label:'Open full chart for selected',icon:'maximize-2',disabled:selIndex<0,onPress:()=>{const r=visible[selIndex];if(r&&strategy)onOpenFull(r,strategy)}},
  {label:'Clear card',icon:'x',danger:true,disabled:!strategy,onPress:()=>onChoose(null)},
 ];
 const cardName=`Scanner ${slot}`;
 const header=<>
  <Pressable ref={titleRef} accessibilityRole="button" accessibilityLabel={strategy?`${cardName}: ${strategy.name}. Change strategy`:`${cardName}: choose a strategy`} {...webOnly({'aria-haspopup':'dialog','aria-expanded':pickerOpen})} onPress={()=>openPicker(titleRef)} style={(st:any)=>[s.row,{flex:1,minWidth:0,gap:6,minHeight:36,borderRadius:6,paddingHorizontal:4,marginLeft:-4,backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}>
   <View aria-hidden style={{width:18,height:18,borderRadius:4,borderWidth:1,borderColor:C.line,alignItems:'center',justifyContent:'center'}}><T style={{fontSize:10,lineHeight:12,fontFamily:'InterSemi',color:C.muted}}>{slot}</T></View>
   <T numberOfLines={2} style={{flexShrink:1,fontFamily:'InterSemi',fontSize:13,lineHeight:16,color:strategy?C.ink:C.muted}}>{strategy?strategy.name:'Choose a strategy'}</T><Icon name="chevron-down" size={14} color={C.muted}/>
  </Pressable>
  <IconButton ref={menuRef} icon="more-vertical" size={14} label={`${cardName} options`} tooltip="Options" haspopup="menu" expanded={menuOpen} onPress={()=>setMenuOpen(true)} style={{width:28,height:28}}/>
  {!!strategy&&<IconButton icon="x" size={14} label={`Clear ${cardName}`} tooltip="Clear card" onPress={()=>onChoose(null)} style={{width:28,height:28}}/>}
 </>;
 // Narrow cards (< 400 px, e.g. 4-across) use the one-line status; the full text stays in the ⓘ popover and the accessibility label.
 const [statusW,setStatusW]=useState(0),narrow=statusW>0&&statusW<400;
 const statusIn={phase:res.phase,universe:uni,date,found:total,shown,revealing};
 // A researched strategy has no live detector yet, so its status line reports three named numbers and never
 // prints the size of the history list as "N found" (that is what `statusText` means by `found`).
 const rst=res.data as any;
 const researchIn={phase:res.phase,date,researched:total,shown,revealing,
  live_detection:!!rst?.live_detection,detections_today:rst?.strategy?.detections_today??null,
  detections_week:rst?.strategy?.detections_week??null,detections_live:rst?.strategy?.detections_live??null,pending:!!rst?.strategy?.evidence_pending};
 // The live list has its own status line: it counts detections, not researched stocks, and says so.
 // The stored scan can be superseded rather than broken: the server then answers 200 with an empty set AND
 // the reason. Either way the card prints the reason instead of "no stock matches this strategy".
 // Declared ABOVE `say`, which reads it and is called during this render by `full`/`text`.
 const reason=((res.data as any)?.unavailable as string|undefined)||unavailable||null;
 // ONE count per card: the Now tab's number comes from one summary, and no other line on the card repeats it.
 const liveSummary:any=(det.data as any)?.strategy||rst?.strategy||strategy||{};
 const activeCount:number|null=liveSummary.detections_live??null;
 const liveIn={phase:det.phase,total:detTotal,shown:detRows.length,lastDetected:liveSummary.detections_last_detected??null,
  available:!!canLive,reason:liveOffText(liveState)};
 // A stored block on the researched pattern set is not an error: the server's own reason replaces the
 // generic "Results unavailable" so the card never reads as broken when it is simply superseded.
 const say=(short:boolean)=>listMode==='live'?liveStatusText(liveIn,short)
  :reason&&(res.phase==='error'||!total)?(short?'Not in use right now':reason)
  :research?researchStatusText(researchIn,short):statusText(statusIn,short);
 const full=strategy?say(false):'',text=strategy?say(narrow):'';
 const coverage=research?evidenceCoverageText(rst?.coverage):null;
 // The live list has no staggered reveal and no history search, so its status line carries no `extra`.
 const extra=listMode==='live'?'':`${cut?` · showing ${shown}`:''}${missing?` · ${missing} is not in this list`:''}`;
 const failedOrBusy=busy||failed||(listMode!=='live'&&(revealing||cut));
 const statusRow=!canLive||!!text||!!extra||failedOrBusy;
 // When the status line has nothing to say (a complete Now list), the ⓘ and ↻ buttons sit at the end of the tab row
 // instead, so the card spends no empty row on them.
 const tools=strategy?<>
   <IconButton ref={infoRef} icon="info" size={14} label={`About ${strategy.name}`} tooltip="What this strategy is" haspopup="dialog" expanded={infoOpen} onPress={()=>setInfoOpen(true)} style={{width:30,height:30}}/>
   <IconButton icon="refresh-cw" size={13} label={`Reload ${strategy.name}`} tooltip="Reload" disabled={busy} onPress={()=>{res.reload();if(canLive)det.reload()}} style={{width:30,height:30}}/>
 </>:null;
 const small={minHeight:28,paddingHorizontal:8,borderRadius:6,justifyContent:'center' as const};
 let body:React.ReactNode;
 if(strategy&&listMode==='live'){
  // The live list is its own source with its own phases; it never falls back to the history rows, because a
  // researched-history row is not a detection and must never be presented as one.
  if(det.phase==='error')body=<CenterNote icon="alert-triangle" text="No list is shown while current setups are unavailable. This is not the same as no setups."/>;
  else if(det.phase==='stopped')body=<CenterNote icon="pause" text="Stopped. Press ↻ to load current setups."/>;
  else if(det.phase==='loading'||!det.data)body=<View aria-hidden style={{padding:12,gap:12}}>{[0,1,2,3,4,5].map(i=><View key={i} style={[s.row,{gap:8}]}><View style={{flex:1,gap:4}}><View style={{height:10,width:'45%',borderRadius:3,backgroundColor:C.line}}/><View style={{height:8,width:'70%',borderRadius:3,backgroundColor:C.line,opacity:.6}}/></View><View style={{width:40,height:10,borderRadius:3,backgroundColor:C.line}}/></View>)}</View>;
 // Never a bare blank panel: say nothing is standing and offer the history in one click.
  else if(!detRows.length)body=<CenterNote icon="search" text={full}>
   <Pressable accessibilityRole="button" accessibilityLabel={`Show history for ${strategy.name}`} onPress={()=>chooseMode('history')} style={(st:any)=>[{minHeight:32,paddingHorizontal:12,borderRadius:8,borderWidth:1,justifyContent:'center',borderColor:C.line,backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}><T style={{fontSize:12,fontFamily:'InterSemi',color:C.ink}}>Show history</T></Pressable>
  </CenterNote>;
  else body=<LiveList rows={detRows} total={detTotal} session={session} selectedId={selectedDetectionId||null} label={strategy.name}
   onSelect={d=>onSelectDetection?.(d,strategy)} onOpenFull={d=>(onOpenDetection||onSelectDetection)?.(d,strategy)}/>;
 }
 else if(!strategy)body=<CenterNote icon="list" text={`Pick one of ${strategies.length} strategies to list ${universeLabel} stocks.`}><Pressable ref={ctaRef} accessibilityRole="button" accessibilityLabel={`Choose a strategy for ${cardName}`} {...webOnly({'aria-haspopup':'dialog','aria-expanded':pickerOpen})} onPress={()=>openPicker(ctaRef)} style={(st:any)=>[s.row,{gap:6,minHeight:36,paddingHorizontal:14,borderRadius:10,borderWidth:1,borderColor:C.green,backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}><T style={{fontSize:13,fontFamily:'InterSemi',color:C.green}}>Choose a strategy</T><Icon name="chevron-down" size={14} color={C.green}/></Pressable></CenterNote>;
 else if(res.phase==='error')body=<CenterNote icon={reason?'info':'alert-triangle'}
  text={reason||'No list is shown while results are unavailable. This is not the same as zero matches.'}/>;
 else if(res.phase==='stopped')body=<CenterNote icon="pause" text="Stopped. Press ↻ to load the stored scan."/>;
 else if(inFlight||(res.phase==='done'&&!res.data))body=<View aria-hidden style={{padding:12,gap:12}}>{[0,1,2,3,4,5].map(i=><View key={i} style={[s.row,{gap:8}]}><View style={{flex:1,gap:4}}><View style={{height:10,width:'45%',borderRadius:3,backgroundColor:C.line}}/><View style={{height:8,width:'70%',borderRadius:3,backgroundColor:C.line,opacity:.6}}/></View><View style={{width:40,height:10,borderRadius:3,backgroundColor:C.line}}/></View>)}</View>;
 else if(!rows.length)body=<CenterNote icon={reason?'info':'search'} text={reason||full}/>;
 else body=<>
  <View style={[s.row,{paddingHorizontal:12,minHeight:24,gap:5}]}><T numberOfLines={1} style={[head,{flex:1,minWidth:0}]}>Stock</T><T numberOfLines={1} style={[head,{width:COL.low,textAlign:'right'}]}>{cols.low}</T><T numberOfLines={1} style={[head,{width:COL.avg,textAlign:'right'}]}>{cols.avg}</T><T numberOfLines={1} style={[head,{width:COL.n,textAlign:'right'}]}>{cols.n}</T></View>
  <ScrollView style={{flex:1}} contentContainerStyle={{paddingBottom:6}}>
   <View aria-label={`${strategy.name}: ${total} stocks`} {...webOnly({role:'listbox'})}>
    {visible.map((r,i)=>{const on=i===selIndex,note=rowSampleNote(r),pending=research&&rowPending(r as any);return <Pressable key={r.match_id||`${r.symbol}:${r.timeframe}`} ref={(el:any)=>{rowRefs.current[i]=el}} role="option" aria-selected={on} accessibilityState={{selected:on}}
     accessibilityLabel={pending?`${r.symbol}, ${r.company}. ${ROW_PENDING}${on?', selected':''}`:`${r.symbol}, ${r.company}. ${cols.low} ${pct(r.low_pct)}, average ${pct(r.avg_pct)} after costs, ${r.n} ${research?'tested trades':'trades'}, ${research?plainEvidence((r as any).evidence_state):r.sample_label}${on?', selected':''}`}
     {...webOnly({tabIndex:(selIndex<0?i===0:on)?0:-1,onKeyDown:(e:any)=>rowKey(i,e)})} onPress={()=>onSelect(r,strategy)}
     style={(st:any)=>[s.row,{gap:5,minHeight:44,paddingHorizontal:8,marginHorizontal:3,marginVertical:1,borderRadius:8,borderWidth:1,borderColor:on?C.green:'transparent',backgroundColor:on?C.soft:st.hovered?'#0F1F28':'transparent'},st.focused&&web?({outlineStyle:'solid',outlineWidth:2,outlineColor:C.mint,outlineOffset:-1} as any):null]}>
     {/* Nothing is cut: the symbol keeps its own width and the company wraps onto a second line instead of truncating. */}
     <View style={{flex:1,minWidth:0,paddingVertical:3}}><T style={{fontSize:13,lineHeight:17,fontFamily:'InterSemi',color:on?C.mint:C.ink}}>{r.symbol}</T><T style={{fontSize:11,lineHeight:14,color:C.muted}}>{r.company}</T></View>
     {pending
      ?<View style={{width:COL.low+COL.avg+COL.n+10,flexShrink:0,alignItems:'flex-end'}}><T numberOfLines={1} style={{fontSize:10,lineHeight:16,color:C.muted}}>{ROW_PENDING}</T></View>
      :<>
       <T numberOfLines={1} style={[tab,{width:COL.low,flexShrink:0,textAlign:'right',fontSize:12,lineHeight:16,fontFamily:'InterSemi',color:tone(r.low_pct)}]}>{pct(r.low_pct)}</T>
       <T numberOfLines={1} style={[tab,{width:COL.avg,flexShrink:0,textAlign:'right',fontSize:12,lineHeight:16,color:tone(r.avg_pct)}]}>{pct(r.avg_pct)}</T>
       <View style={{width:COL.n,flexShrink:0,alignItems:'flex-end'}}><T style={[tab,{fontSize:12,lineHeight:16}]}>{r.n==null?'—':r.n}</T>{!!note&&<T numberOfLines={1} style={{fontSize:9,lineHeight:12,color:C.amber}}>{note==='Small sample'?'small':'none'}</T>}</View>
      </>}
    </Pressable>})}
   </View>
  </ScrollView>
 </>;
 // Coverage sits in the footer so the honest "how much of this is ready" line is always visible while the
 // outcome run is still working, without competing with the status line for space.
 const footer=strategy&&(coverage||res.data?.stale)?<>
  {!!coverage&&<T style={{fontSize:11,lineHeight:15,color:C.muted}}>{coverage}</T>}
  {!!res.data?.stale&&<StaleLine stale ageDays={res.data.age_days}/>}
 </>:null;
 return <CardShell label={strategy?`${cardName}, ${strategy.name}`:cardName} style={style} header={header} footer={footer}>
  <View aria-hidden style={{height:2,overflow:'hidden',backgroundColor:busy||(listMode!=='live'&&revealing)?C.line:'transparent'}}>
   {busy&&<Animated.View style={{position:'absolute',top:0,bottom:0,width:'30%',backgroundColor:C.green,left:reduce?'35%':slide.interpolate({inputRange:[0,1],outputRange:['-30%','100%']})}}/>}
   {listMode!=='live'&&revealing&&<View style={{height:2,width:`${Math.round(shown/Math.max(1,rows.length)*100)}%` as any,backgroundColor:C.green}}/>}
  </View>
  {/* Two tabs, one count each: "Now" = setups standing right now; "History" = stocks where the pattern happened
      before. No other line on the card repeats either number. */}
  {!!strategy&&canLive&&<View role="tablist" aria-label={`${cardName} list`} style={[s.row,{gap:6,rowGap:6,flexWrap:'wrap',paddingHorizontal:8,paddingTop:6}]}>
   {(['live','history'] as ListMode[]).map(m=>{const on=listMode===m;
    return <Pressable key={m} role="tab" aria-selected={on} accessibilityState={{selected:on}}
     accessibilityLabel={m==='live'?`${LIST_MODE_LABELS.live}, ${activeCount??'unknown'} setups`:`${LIST_MODE_LABELS.history}, ${res.data?total:((strategy as any).researched_stocks??0)} stocks`}
     {...webOnly({tabIndex:on?0:-1})} onPress={()=>chooseMode(m)}
     style={(st:any)=>[{minHeight:26,paddingHorizontal:9,borderRadius:7,borderWidth:1,justifyContent:'center',borderColor:on?C.green:C.line,backgroundColor:on?C.soft:st.hovered||st.focused?C.soft:'transparent'}]}>
     <T numberOfLines={1} style={{fontSize:11,lineHeight:15,fontFamily:'InterSemi',color:on?C.mint:C.muted}}>
      {listModeLabel(m,activeCount,res.data?total:((strategy as any).researched_stocks??null))}</T>
    </Pressable>})}
   {!statusRow&&<><View style={{flex:1}}/>{tools}</>}
  </View>}
  {!!strategy&&statusRow&&<View onLayout={e=>{const w=Math.round(e.nativeEvent.layout.width);setStatusW(v=>Math.abs(v-w)<4?v:w)}} style={[s.row,{gap:0,paddingLeft:4,paddingRight:6,minHeight:34}]}>
   {tools}
   <View style={[s.row,{flex:1,minWidth:0,gap:5}]}>{res.phase==='error'&&<Icon name="alert-triangle" size={13} color={C.amber}/>}<T accessibilityLiveRegion="polite" accessibilityLabel={full+extra} {...webOnly({'aria-live':'polite'})} numberOfLines={2} style={{flexShrink:1,fontSize:11,lineHeight:15,color:res.phase==='error'?C.amber:C.muted}}>{text}{extra}</T></View>
   {(busy||(listMode!=='live'&&revealing))&&<Pressable accessibilityRole="button" accessibilityLabel={busy?'Stop loading':'Stop revealing rows'} onPress={()=>busy?(listMode==='live'?det.stop():res.stop()):setReveal(r=>({...r,stopped:true}))} style={(st:any)=>[s.row,small,{gap:4,backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}><Icon name="pause" size={12} color={C.ink}/><T style={{fontSize:11,fontFamily:'InterSemi'}}>Stop</T></Pressable>}
   {failed&&<Pressable accessibilityRole="button" accessibilityLabel={`Retry ${strategy.name}`} onPress={listMode==='live'?det.reload:res.reload} style={(st:any)=>[small,{backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}><T style={{fontSize:11,fontFamily:'InterSemi',color:C.green}}>Retry</T></Pressable>}
   {listMode!=='live'&&cut&&<Pressable accessibilityRole="button" accessibilityLabel={`Show all ${rows.length} stocks`} onPress={()=>setReveal(r=>({...r,shown:rows.length,stopped:false}))} style={(st:any)=>[small,{backgroundColor:st.hovered||st.focused?C.soft:'transparent'}]}><T style={{fontSize:11,fontFamily:'InterSemi',color:C.green}}>Show all</T></Pressable>}
  </View>}
  <View style={{flex:1,minHeight:0}}>{body}</View>
  <StrategyPicker open={pickerOpen} onClose={()=>setPickerOpen(false)} anchor={anchor} strategies={strategies} currentKey={skey} universeLabel={universeLabel} cardLabel={cardName} onChoose={k=>onChoose(k)}/>
  <Popover open={menuOpen} onClose={()=>setMenuOpen(false)} anchor={menuRef} placement="bottom-end" label={`${cardName} options`} role="none" width={250}><MenuList items={menu} onClose={()=>setMenuOpen(false)} label={`${cardName} options`}/></Popover>
  {!!strategy&&<Popover open={infoOpen} onClose={()=>setInfoOpen(false)} anchor={infoRef} placement="bottom-start" label={`About ${strategy.name}`} width={340}>
   <View style={{paddingHorizontal:14,paddingVertical:8,gap:6}}>
    <T style={{fontFamily:'InterSemi',fontSize:13}}>{strategy.name}</T>
    {!!full&&<T style={{fontSize:12,lineHeight:17,color:C.muted}}>{full}{extra}</T>}
    {!!coverage&&<T style={{fontSize:12,lineHeight:17,color:C.muted}}>{coverage}</T>}
    {!!strategy.description&&<T style={{fontSize:12,lineHeight:17,color:C.muted}}>{strategy.description}</T>}
    <T style={{fontSize:12,lineHeight:18}}>Pattern: {strategy.pattern_name} · Timeframe: {strategy.timeframe} · Side: {strategy.side==='long'?'Long (bullish)':'Short (bearish)'} · Universe: {universeLabel}</T>
    {research
     ?<T style={{fontSize:12,lineHeight:18}}>Now lists setups forming or confirmed right now; "new" marks one first found in the latest session. History lists stocks where this pattern happened before. "Edge low" is a cautious (95% low) estimate of how much better the pattern did than buying the same stock on any day, over the same holding period, after costs. "Avg" is the average return after costs on data the pattern was not tuned on, and "Trades" is how many of those trades there were. Fewer than {strategy.min_trades} trades is a small sample and is never ranked on.</T>
     :<T style={{fontSize:12,lineHeight:18}}>Evidence basis: stored scan matches on data to {date}. 95% low and Avg are this pattern's hold-period history on each stock after costs — not a tested exit rule unless the Backtest card says "Tested exit rule". Rows below {strategy.min_trades} trades are small samples.</T>}
    <T style={{fontSize:12,lineHeight:18}}>Costs: 0.40% round trip included. Entry: next open after the signal candle. Win rate is context only; ranking is by 95% low.</T>
   </View>
  </Popover>}
 </CardShell>;
}
