import React,{useMemo,useState} from 'react';
import {View,ScrollView,TextInput,useWindowDimensions} from 'react-native';
import {router,useLocalSearchParams} from 'expo-router';
import {C,T,s,Button,Chip,Badge} from '../ui';
import {api} from '../model';
import {useKeyedValue} from '../activeSymbol';
import {PatternCanvas} from '../PatternCanvas';
import {WidgetError} from '../layout';
import {Skeleton} from '../discover/parts';
import {drawingCatalogue,exampleKey,exampleSnapshot,familyName,variantKey,PAGE_SIZE,type DrawingAudit,type DrawingExample,type CatalogueRow} from './logic';

const one=(v:unknown)=>typeof v==='string'?v:Array.isArray(v)?String(v[0]||''):'';
const date=(v:string)=>v?.slice(0,10)||'Unavailable';
type ReviewFinding={id:string;scope:string;observation:string;status:string};
type SampleReview={event_identity:Record<string,unknown>;frozen_event_sha256:string;inspected:boolean;finding_ids:string[];checks:{post_fix_visual_recheck:string}};
type VisualReview={run:string;drawing_adapter_sha256?:string;coverage:{patterns_total:number;patterns_inspected:number;manual_examples_inspected:number;available_directional_variants:number};findings:ReviewFinding[];by_pattern:Record<string,string>;reviews_by_event_identity:Record<string,SampleReview>};
/** Reviews bind to an exact frozen event, never to a pattern name alone. */
function reviewFor(example:DrawingExample,review?:VisualReview):SampleReview|undefined{
 if(!review||review.run!==example.source.run)return;
 const stored=example as DrawingExample&{event_identity?:Record<string,unknown>;event_sha256?:string};
 const value=Object.values(review.reviews_by_event_identity).find(r=>r.frozen_event_sha256===stored.event_sha256&&Object.keys(r.event_identity).every(k=>r.event_identity[k]===stored.event_identity?.[k]));
 if(!value||!stored.event_identity||!stored.event_sha256||stored.event_sha256!==value.frozen_event_sha256)return;
 const keys=Object.keys(value.event_identity);if(keys.length!==Object.keys(stored.event_identity).length||keys.some(k=>value.event_identity[k]!==stored.event_identity![k]))return;
 return value;
}
function ReviewNote({example,review,compact=false}:{example:DrawingExample;review?:VisualReview;compact?:boolean}){
 if(!review)return null;
 const recorded=reviewFor(example,review),pending=recorded?.checks.post_fix_visual_recheck==='pending',findings=recorded?review.findings.filter(f=>recorded.finding_ids.includes(f.id)):[];
 return <View style={{gap:4}}><T style={{fontSize:11,color:pending||!recorded?C.amber:C.muted}}>{recorded?.inspected?`This exact example was inspected${pending?' · visual recheck pending':''}.`:'This example has no recorded visual inspection.'}</T>{findings.filter(f=>!compact||f.status.includes('unresolved')).map(f=><T key={f.id} style={{fontSize:12,color:C.amber}}>{f.observation} ({f.status.replace(/_/g,' ')})</T>)}</View>;
}
function FrozenChart({example,compact=false}:{example:DrawingExample;compact?:boolean}){
 const {match,snapshot}=useMemo(()=>exampleSnapshot(example),[example]);
 return <PatternCanvas key={exampleKey(example)} match={match} side={example.spec.side} snapshot={snapshot} compact={compact} controls={!compact} chartHeight={compact?220:380}/>;
}
function Availability({row}:{row:CatalogueRow}){return <Badge tone={row.examples.length?'neutral':'amber'} label={row.examples.length?'Real example available':'No frozen occurrence'}/>;}

export function DrawingReviewPage(){
 const params=useLocalSearchParams(),{width}=useWindowDimensions(),wide=width>=1000;
 const [search,setSearch]=useState(one(params.q)),[family,setFamily]=useState(one(params.family)||'all'),[status,setStatus]=useState('all');
 const [view,setView]=useState(one(params.view)==='detail'?'detail':'grid'),[page,setPage]=useState(Math.max(0,(Number(one(params.page))||1)-1)),[allVariants,setAllVariants]=useState(one(params.scope)==='variants');
 const [pattern,setPattern]=useState(one(params.pattern_id)||'CH16'),[chosen,setChosen]=useState(one(params.example)),[variant,setVariant]=useState('');
 const request=useKeyedValue<DrawingAudit>('pattern-drawings-audit',()=>api('/api/pattern-drawings/audit'));
 const reviewRequest=useKeyedValue<VisualReview>('pattern-drawings-review',()=>api('/api/pattern-drawings/review'));
 const [showNotes,setShowNotes]=useState(false);
 const data=request.value,catalogue=useMemo(()=>data?drawingCatalogue(data):[],[data]);
 const adapter=(data as DrawingAudit&{drawing_adapter_sha256?:string})?.drawing_adapter_sha256;
 const review=reviewRequest.value?.run===data?.run&&reviewRequest.value?.drawing_adapter_sha256===adapter?reviewRequest.value:undefined,quality=review?.findings.filter(f=>f.status.includes('unresolved'))||[];
 const filtered=useMemo(()=>{const q=search.trim().toLowerCase();return catalogue.filter(r=>(family==='all'||r.family===family)&&(status==='all'||(status==='available'?r.examples.length>0:r.missing.length>0||!r.examples.length))&&(!q||`${r.id} ${r.name} ${r.family} ${r.examples.map(e=>`${e.spec.variant} ${e.symbol}`).join(' ')}`.toLowerCase().includes(q)));},[catalogue,search,family,status]);
 const examples=useMemo(()=>filtered.flatMap(r=>allVariants?r.examples:r.examples.slice(0,1)),[filtered,allVariants]),pages=Math.max(1,Math.ceil(examples.length/PAGE_SIZE)),safePage=Math.min(page,pages-1);
 const row=filtered.find(r=>r.id===pattern)||filtered[0],chosenExample=row?.examples.find(e=>exampleKey(e)===chosen);
 const variants=row?[...new Map([...row.examples.map(e=>e.spec),...row.missing.map(e=>e.spec)].map(spec=>[variantKey(spec),spec])).values()]:[];
 const selectedVariant=variants.some(spec=>variantKey(spec)===variant)?variant:chosenExample?variantKey(chosenExample.spec):variants[0]?variantKey(variants[0]):'';
 const sameVariant=row?.examples.filter(e=>variantKey(e.spec)===selectedVariant)||[];
 const example=sameVariant.find(e=>exampleKey(e)===chosen)||sameVariant[0];
 const setMode=(next:string)=>{setView(next);router.setParams({view:next,page:String(safePage+1)})};
 const changePage=(next:number)=>{setPage(next);router.setParams({view:'grid',page:String(next+1)})};
 const select=(e:DrawingExample)=>{setPattern(e.spec.pattern_id);setChosen(exampleKey(e));setVariant(variantKey(e.spec));setView('detail');router.setParams({pattern_id:e.spec.pattern_id,example:exampleKey(e),view:'detail'})};
 const resetPage=()=>{setPage(0);router.setParams({page:'1'})};
 const columns=width>=1250?3:width>=800?2:1;
 return <View style={{padding:width<700?14:24,gap:18,maxWidth:1700,width:'100%',alignSelf:'center'}}>
  <View style={[s.between,{flexWrap:'wrap'}]}><View style={{gap:5,flexShrink:1}}><Badge label="Frozen drawing review" tone="neutral"/><T role="heading" aria-level={1} style={s.title}>Inspect every pattern drawing.</T><T style={{color:C.muted}}>Real historical candles, the original formation, and its stored drawing.</T></View><Button label="Back to Discover" kind="outline" icon="arrow-left" onPress={()=>router.push('/discover')}/></View>
  {request.error?<WidgetError title="Drawing examples unavailable" message={request.error} onRetry={request.reload}/>:!data?<Skeleton lines={6}/>:<>
   <View style={[s.card,{padding:16,gap:8}]}><T style={{fontFamily:'InterSemi'}}>{data.coverage.patterns_with_examples} of {data.coverage.catalogue_entries} patterns · {data.coverage.variants_with_examples} of {data.coverage.directional_variants} directional variants have real examples</T>
    {review?<><T style={{fontSize:12}}>Recorded visual review: {review.coverage.manual_examples_inspected} of {review.coverage.available_directional_variants} directional examples across {review.coverage.patterns_inspected} of {review.coverage.patterns_total} pattern IDs.</T><T style={{color:C.muted,fontSize:12}}>Each review is tied to the exact frozen event shown. This does not cover every historical occurrence or every timeframe, and does not certify detector validity.</T>{quality.map(f=><T key={f.id} style={{color:C.amber,fontSize:12}}>Open concern · {f.scope}: {f.observation}</T>)}<Button label={`${showNotes?'Hide':'Show'} review notes (${review.findings.length})`} kind="ghost" accessibilityState={{expanded:showNotes}} onPress={()=>setShowNotes(v=>!v)} style={{alignSelf:'flex-start'}}/>{showNotes&&review.findings.map(f=><View key={f.id} style={{gap:2}}><T style={{fontSize:12,fontFamily:'InterSemi',color:C.amber}}>{f.scope} · {f.status.replace(/_/g,' ')}</T><T style={{fontSize:12,color:C.muted}}>{f.observation}</T></View>)}</>:<T style={{color:C.amber,fontSize:12}}>{reviewRequest.loading?'Loading the visual review record…':'Visual review status is unavailable for this frozen run. Example availability alone is not an accuracy verdict.'}</T>}
    <T style={{fontSize:12,color:C.muted}}>One representative timeframe per variant and direction. Candles stop at the signal. These examples do not show what happened afterward.</T></View>
   <View style={{gap:10}}><TextInput accessibilityLabel="Search pattern drawing catalogue" placeholder="Search pattern name, ID, variant or stock" placeholderTextColor={C.muted} value={search} onChangeText={v=>{setSearch(v);resetPage()}} style={s.input}/>
    <View style={[s.row,{flexWrap:'wrap'}]}>{['all',...new Set(catalogue.map(r=>r.family))].map(f=><Chip key={f} label={f==='all'?'All families':familyName(f)} active={family===f} onPress={()=>{setFamily(f);resetPage();router.setParams({family:f})}}/>)}</View>
    <View style={[s.between,{flexWrap:'wrap'}]}><View style={[s.row,{flexWrap:'wrap'}]}>{[['all','All availability'],['available','Examples available'],['missing','Missing occurrences']].map(([value,label])=><Chip key={value} label={label} active={status===value} onPress={()=>{setStatus(value);resetPage()}}/>)}</View><View style={s.row}><Button label="Single drawing" kind={view==='detail'?'soft':'ghost'} accessibilityState={{selected:view==='detail'}} onPress={()=>setMode('detail')}/><Button label="Drawing grid" kind={view==='grid'?'soft':'ghost'} accessibilityState={{selected:view==='grid'}} onPress={()=>setMode('grid')}/></View></View>
    <T accessibilityLiveRegion="polite" style={{fontSize:12,color:C.muted}}>{filtered.length} patterns · {filtered.reduce((n,r)=>n+r.examples.length,0)} available directional variants · one observed timeframe per example</T>
   </View>
   {view==='grid'?<>
    <View style={[s.row,{flexWrap:'wrap'}]}><Chip label={`One per pattern (${filtered.filter(r=>r.examples.length).length})`} active={!allVariants} onPress={()=>{setAllVariants(false);resetPage();router.setParams({scope:'patterns'})}}/><Chip label={`All variants (${filtered.reduce((n,r)=>n+r.examples.length,0)})`} active={allVariants} onPress={()=>{setAllVariants(true);resetPage();router.setParams({scope:'variants'})}}/></View>
    <View style={s.between}><T style={{fontFamily:'InterSemi'}}>Examples {examples.length?safePage*PAGE_SIZE+1:0}–{Math.min(examples.length,(safePage+1)*PAGE_SIZE)} of {examples.length}</T><View style={s.row}><Button label="Previous page" kind="outline" disabled={safePage===0} onPress={()=>changePage(safePage-1)}/><T style={{fontSize:12}}>Page {safePage+1} / {pages}</T><Button label="Next page" kind="outline" disabled={safePage+1>=pages} onPress={()=>changePage(safePage+1)}/></View></View>
    <View style={{flexDirection:'row',flexWrap:'wrap',gap:12}}>{examples.slice(safePage*PAGE_SIZE,(safePage+1)*PAGE_SIZE).map(e=><View key={exampleKey(e)} accessibilityLabel={`Drawing example ${e.spec.pattern_id} ${e.spec.variant} ${e.spec.side}`} style={[s.card,{width:columns===3?'32%':columns===2?'48.8%':'100%',padding:12,gap:8}]}><T role="heading" aria-level={2} style={{fontFamily:'InterSemi',fontSize:14}}>{e.spec.pattern_id} · {e.spec.name}</T><T style={{fontSize:11,color:C.muted}}>{e.event.state==='confirmed'?'Confirmed':'Forming / setup'} · {e.spec.variant} · {e.spec.side} · {e.symbol} · {e.timeframe} · {date(e.signal_at)}</T><FrozenChart example={e} compact/><ReviewNote example={e} review={review} compact/><Button label={`Inspect ${e.spec.pattern_id}`} kind="outline" onPress={()=>select(e)}/></View>)}</View>
    {!examples.length&&<T style={{color:C.muted}}>No frozen examples match these filters. Missing occurrences remain listed in the single drawing view.</T>}
   </>:<View style={{flexDirection:wide?'row':'column',gap:18,alignItems:'stretch'}}>
    <View style={[s.card,{width:wide?290:undefined,padding:12,gap:8,alignSelf:wide?'flex-start':undefined}]}><T style={{fontFamily:'InterSemi'}}>Pattern catalogue · {filtered.length}</T><ScrollView style={{maxHeight:wide?660:230}} contentContainerStyle={{gap:5}}>{filtered.map(r=><View key={r.id} style={{gap:2,paddingBottom:6}}><Button label={`${r.id} · ${r.name}`} accessibilityLabel={`${r.id}, ${r.name}, ${r.examples.length} real examples, ${r.missing.length} missing variants`} kind={r.id===row?.id?'soft':'ghost'} accessibilityState={{selected:r.id===row?.id}} onPress={()=>{setPattern(r.id);setChosen('');setVariant('');router.setParams({pattern_id:r.id,example:undefined})}} style={{justifyContent:'flex-start'}}/><View style={{paddingLeft:10}}><Availability row={r}/></View></View>)}{!filtered.length&&<T style={{color:C.muted}}>No patterns match these filters.</T>}</ScrollView></View>
    <View style={[s.card,{flex:1,minWidth:0,padding:width<700?12:20}]}>{row?<><View style={{gap:4}}><T role="heading" aria-level={2} style={{fontFamily:'ManropeBold',fontSize:24,lineHeight:32}}>{row.id} · {row.name}</T><T style={{fontSize:12,color:C.muted}}>{familyName(row.family)} · {row.examples.length} real examples · {row.missing.length} missing variants</T></View>
     <View style={[s.row,{flexWrap:'wrap'}]}>{variants.map(spec=><Chip key={variantKey(spec)} label={`${spec.variant} · ${spec.side}`} active={selectedVariant===variantKey(spec)} onPress={()=>{setVariant(variantKey(spec));const e=row.examples.find(v=>variantKey(v.spec)===variantKey(spec));setChosen(e?exampleKey(e):'')}}/>)}</View>
     {sameVariant.length>0&&<View style={[s.row,{flexWrap:'wrap'}]}>{sameVariant.map(e=><Chip key={exampleKey(e)} label={`${e.timeframe} · ${e.symbol} · ${date(e.signal_at)}`} active={e===example} onPress={()=>setChosen(exampleKey(e))}/>)}</View>}
     {example?<><View style={[s.between,{flexWrap:'wrap'}]}><Badge label="Real example available" tone="neutral"/><T style={{fontSize:12,color:C.muted}}>{example.event.state} · {example.symbol} · {example.timeframe} · signal {date(example.signal_at)}</T></View><FrozenChart example={example}/><ReviewNote example={example} review={review}/><T style={{fontSize:12,color:C.muted}}>Formation {date(example.bars[example.formation_start]?.time)} · shape detected {date(example.detected_at)} · signal candle {date(example.signal_at)}</T><View style={{gap:6}}><T style={{fontFamily:'InterSemi',fontSize:13}}>Pattern definition</T><T style={{fontSize:12,color:C.muted}}>{example.spec.definition||'No definition supplied for this variant.'}</T></View><T style={{fontSize:11,color:C.muted}}>Frozen run {example.source.run} · {example.bars.length} candles · source {example.drawing_source.replace(/_/g,' ')}</T></>:<T style={{color:C.amber}}>No occurrence is available in this frozen research data. A drawing has not been substituted.</T>}
     {row.missing.map((m,i)=><T key={variantKey(m.spec)+i} style={{color:C.amber,fontSize:12}}>{m.spec.variant} · {m.spec.side}: {m.reason||m.note||'No frozen occurrence available.'}</T>)}
    </>:<T style={{color:C.muted}}>Choose a pattern from the catalogue.</T>}</View>
   </View>}
   <T style={{color:C.muted,fontSize:11}}>Source: frozen research run {data.run}. {data.method}</T>
  </>}
 </View>;
}
