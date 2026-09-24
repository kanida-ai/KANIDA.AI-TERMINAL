// THE WIDGETS. Each one is the product's existing block or panel, pointed at the workspace's instrument — or a
// small view over a route that already serves real data. None of them fetches on its own schedule: every read goes
// through the app's `api`, and so through the page's hub (hub.ts), which shares identical reads and keeps them for
// the reading. The Derivative tab's files are imported, never edited.
import React,{useEffect,useMemo,useState} from 'react';
import {Pressable,ScrollView,TextInput,View} from 'react-native';
import Svg,{Rect,Line as SvgLine} from 'react-native-svg';
import {Badge,Button,C,Icon,T,s} from '../ui';
import {useDerivativeRead} from '../derivative/useDerivatives';
import {ChainWidget} from '../derivative/ChainWidget';
import {OiByStrikeSection} from '../derivative/OiByStrikeSection';
import {OiGridSection} from '../derivative/OiGridSection';
import {PcrSection,MaxPainSection,IvSection,FuturesBuildupSection} from '../derivative/SessionBlocks';
import {FuturesChartPanel} from '../derivative/FuturesChartPanel';
import {IndexWidget} from '../derivative/IndexWidget';
import {FuturesWidget} from '../derivative/FuturesWidget';
import {WhatsHappening} from '../derivative/WhatsHappening';
import {SignalPanel} from '../derivative/SignalPanel';
import {SignalTable} from '../derivative/SignalTable';
import {SCREENER_PATH,SCREENER_VIEW_DEFAULT,screenerQuery} from '../derivative/logic';
import type {ChartTarget,OiGrid} from '../derivative/types';
import {Builder} from '../screener/Builder';
import {screenerApi} from '../screener/api';
import {STATUS_LABEL,STATUS_TONE,expiryShort,newDefinition} from '../screener/model';
import type {Definition as ScanDef,Match,Parsed,Results,Scanner,Vocabulary} from '../screener/types';
import {CHROME,TYPE} from './tokens';
import * as Frame from '../derivative/frame';
import {expiryOf,instrumentOf,useWorkspace} from './context';
import {wsApi} from './api';
import type {Summary,Widget,WidgetSpec} from './types';

export type BodyProps={w:Widget;spec:WidgetSpec;width:number;height:number;expanded:boolean;
 onPatch:(patch:Partial<Widget>)=>void;openSettings:()=>void};

const q=(v:string)=>encodeURIComponent(v);
const withExpiry=(path:string,expiry:string)=>expiry?`${path}&expiry=${q(expiry)}`:path;
const NONE=()=>{};

/** The lines a widget prints when it has nothing to read yet. Never a zero, never a spinner forever. */
export function Idle({icon='crosshair',title,detail}:{icon?:string;title:string;detail?:string}){
 return <View style={{flex:1,alignItems:'center',justifyContent:'center',gap:8,padding:20}}>
  <Icon name={icon} size={20} color={C.muted}/>
  <T style={[TYPE.title,{textAlign:'center'}]}>{title}</T>
  {!!detail&&<T style={[TYPE.body,{color:C.muted,textAlign:'center',maxWidth:320}]}>{detail}</T>}
 </View>;
}
const NO_SYMBOL=<Idle title="No instrument selected" detail="Pick one from a screener, the market scan or the instrument chip above — every connected widget follows."/>;

/** The same symbol a block shows in its badge: the workspace's selection is always a CHOICE, never a default. */
// A WIDGET FITS ITS BOX (scroll.ts). A two-panel Derivative block is laid out to its tile: side by side when the
// tile is wide enough for both, ONE panel when it is not (the chart — the block's headline already carries the
// latest figure), and panels exactly as tall as the space under that headline. So a PCR widget is a chart that
// fills its box, not a chart and a table stacked into a scrolling column. The user's own "Show" choice in ⚙ wins.
const BESIDE=900;
const Bare=(Frame as any).BlockBareContext;
// what sits above the panels: the headline alone once the after-close hook is in, the block's title row as well before it
const ABOVE=Bare?64:128;
function fit(width:number,height:number,view:string|undefined,chartKey:string,readKey:string){
 const auto=!view||view==='auto';
 const one=auto?width<BESIDE:view!=='both';
 const hidden=(auto&&one)||view==='chart'?{[readKey]:true}:view==='readings'?{[chartKey]:true}:{};
 return {hidden,stacked:!one&&width<BESIDE,height:Math.max(180,height-CHROME.pad*2-ABOVE)};
}
function blockProps(symbol:string,expiry:string,seq:number,f:{hidden:Record<string,boolean>;stacked:boolean;height:number},onTarget:(t:ChartTarget)=>void){
 return {symbol,choice:{symbol,defaulted:false,label:''},badge:symbol,linked:true,expiry,seq,stacked:f.stacked,
  height:f.height,hidden:f.hidden,onTarget,target:null};
}
function useSelectFromTarget(){
 const ws=useWorkspace();
 return (t:ChartTarget)=>{if(t?.underlying&&t.underlying!==ws.selected.underlying)ws.select({underlying:t.underlying,expiry:null,focus:null});};
}
function focusStrike(ws:ReturnType<typeof useWorkspace>,symbol:string){
 return ws.strike??(ws.selected.underlying===symbol&&ws.selected.focus?.strikes?.[0]||null);
}

// =================================================================================================================
// SCREENERS — these are what point the workspace at an instrument
// =================================================================================================================
function MatchRow({m,active,onPress}:{m:Match;active:boolean;onPress:()=>void}){
 const again=m.episodes.length>1&&m.status==='new';
 return <Pressable accessibilityRole="button" accessibilityState={{selected:active}} onPress={onPress}
  accessibilityLabel={`${m.title}, ${STATUS_LABEL[m.status]}. Point the workspace at it`}
  style={({pressed})=>({gap:4,paddingHorizontal:10,paddingVertical:9,borderRadius:10,borderWidth:1,
   borderColor:active?C.green:C.line,backgroundColor:active?C.soft:C.dark,opacity:pressed?.75:1})}>
  <View style={[s.between,{gap:6}]}>
   <T numberOfLines={1} style={[TYPE.title,{flex:1}]}>{m.title}<T style={[TYPE.helper,{color:C.muted}]}>  {expiryShort(m.expiry)}</T></T>
   <Badge label={m.status==='ended'?`ENDED ${m.ended_at}`:again?'AGAIN':STATUS_LABEL[m.status].replace(' MATCH','').replace('STILL MATCHING','STILL')} tone={STATUS_TONE[m.status]}/>
  </View>
  <T numberOfLines={2} style={[TYPE.helper,{color:C.muted}]}>{m.because[0]}</T>
  <T style={[TYPE.helper,{color:C.muted}]}>first {m.first_matched} · {m.episode_readings} reading{m.episode_readings===1?'':'s'}</T>
 </Pressable>;
}
function MatchList({body,scannerId,scannerName,loading,error}:{body:Results|null;scannerId:string|null;scannerName:string;loading:boolean;error:string}){
 const ws=useWorkspace();
 if(error)return <Idle icon="alert-circle" title="Could not run this scanner" detail={error}/>;
 if(!body)return <Idle icon="loader" title={loading?'Running the scanner…':'—'}/>;
 if(!body.available)return <Idle icon="clock" title="No readings yet" detail={body.text}/>;
 if(!body.matches.length)return <Idle icon="search" title="Nothing matches right now"
  detail={body.counts?.ended?`${body.counts.ended} matched earlier today and ended.`:'KANIDA checks again at every 15-min reading.'}/>;
 const pick=(m:Match)=>ws.select({underlying:m.underlying,expiry:m.expiry,focus:{side:m.type||null,
  strikes:m.strike!=null?[m.strike]:[],from:m.episode_started,to:m.last_matched,source:scannerName,
  because:m.because.slice(0,2),scanner_id:scannerId,match_key:m.key} as any});
 return <View style={{gap:6}}>
  {body.matches.map(m=><MatchRow key={m.key} m={m} active={(ws.selected.focus as any)?.match_key===m.key} onPress={()=>pick(m)}/>)}
  {body.truncated&&<T style={[TYPE.helper,{color:C.muted}]}>Showing the first {body.shown}.</T>}
 </View>;
}
function useScanners(seq:number){return useDerivativeRead<{scanners:Scanner[]}>('/api/screener/scanners',seq);}

export function ScreenerResultsBody({w,openSettings}:BodyProps){
 const ws=useWorkspace();
 const list=useScanners(ws.seq);
 const id=w.scanner_id;
 const view=(w.settings.view as string)||'active';
 const read=useDerivativeRead<Results>(id?`/api/screener/scanners/${q(id)}/results?view=${view}&limit=100`:null,ws.seq);
 const sc=list.data?.scanners.find(x=>x.id===id);
 if(!id)return <Idle icon="filter" title="Choose a scanner" detail="Open settings to pick a KANIDA scanner or one of yours."/>;
 return <View style={{gap:8}}>
  <Pressable accessibilityRole="button" accessibilityLabel="Change scanner" onPress={openSettings} style={[s.between,{gap:6}]}>
   <T numberOfLines={1} style={[TYPE.title,{flex:1,color:C.green}]}>{sc?.name||id} ▾</T>
   {read.data?.counts&&<T style={[TYPE.helper,{color:C.muted}]}>{read.data.counts.active} active · {read.data.counts.ended} ended</T>}
  </Pressable>
  {!!sc&&<T numberOfLines={2} style={[TYPE.helper,{color:C.muted}]}>{sc.reads_as}</T>}
  <MatchList body={read.data} scannerId={id} scannerName={sc?.name||''} loading={read.phase==='loading'} error={read.error}/>
 </View>;
}

export function ScreenerBuilderBody({width}:BodyProps){
 const ws=useWorkspace();
 const vocab=useDerivativeRead<Vocabulary>('/api/screener/vocabulary',0);
 const [draft,setDraft]=useState<ScanDef>(newDefinition());
 const [text,setText]=useState('');
 const [parsed,setParsed]=useState<Parsed|null>(null);
 const [parsing,setParsing]=useState(false);
 const [readsAs,setReadsAs]=useState('');
 const [err,setErr]=useState('');
 const [results,setResults]=useState<Results|null>(null);
 const [running,setRunning]=useState(false);
 const [name,setName]=useState('');
 const [saved,setSaved]=useState('');
 useEffect(()=>{const t=setTimeout(()=>screenerApi.describe(draft).then(r=>{setReadsAs(r.reads_as);setErr('')},e=>setErr(e?.message||'')),250);
  return ()=>clearTimeout(t)},[draft]);
 if(!vocab.data)return <Idle icon="loader" title="Loading the vocabulary…"/>;
 const parse=async()=>{if(!text.trim())return;setParsing(true);
  try{const r=await screenerApi.parse(text);setParsed(r);setDraft(r.definition)}catch(e:any){setErr(e?.message||'')}finally{setParsing(false)}};
 const run=async()=>{setRunning(true);try{setResults(await screenerApi.run(draft,'active'))}catch(e:any){setErr(e?.message||'')}finally{setRunning(false)}};
 const save=async()=>{try{const sc=await screenerApi.create(name||'My scanner',draft,parsed?'nl':'visual',parsed?text:undefined);
  setSaved(`Saved “${sc.name}” to your scanners.`);ws.openScanner(sc.id)}catch(e:any){setErr(e?.message||'')}};
 return <View style={{gap:12}}>
  <Builder vocab={{...vocab.data,underlyings:(vocab.data as any).underlyings||[]}} draft={draft} onChange={setDraft} readsAs={readsAs}
   error={err} text={text} onText={setText} onParse={parse} parsing={parsing} parsed={parsed} compact={width<700} dense/>
  <View style={[s.row,{flexWrap:'wrap',gap:8}]}>
   <Button label="Run" icon="play" onPress={run} loading={running} disabled={!!err}/>
   <TextInput value={name} onChangeText={setName} placeholder="Name to save as" placeholderTextColor={C.muted} maxLength={60}
    accessibilityLabel="Scanner name" style={[s.input,{minHeight:42,flex:1,minWidth:160}]}/>
   <Button label="Save scanner" icon="save" kind="soft" onPress={save} disabled={!!err}/>
  </View>
  {!!saved&&<T style={[TYPE.body,{color:C.green}]}>{saved}</T>}
  {results&&<MatchList body={results} scannerId={null} scannerName="Draft scanner" loading={running} error=""/>}
 </View>;
}

export function ScannersBody({w}:BodyProps){
 const ws=useWorkspace();
 const list=useScanners(ws.seq);
 const which=(w.settings.list as string)||'all';
 const rows=(list.data?.scanners||[]).filter(x=>which==='all'||(which==='mine'?!x.is_default:x.is_default));
 if(!list.data)return <Idle icon="loader" title="Loading scanners…"/>;
 const open=new Set(ws.widgets.filter(x=>x.widget_type==='screener_results').map(x=>x.scanner_id));
 return <View style={{gap:4}}>
  {!rows.length&&<T style={[TYPE.body,{color:C.muted}]}>No scanners of yours yet — build one in a Screener builder widget.</T>}
  {rows.map(sc=><Pressable key={sc.id} accessibilityRole="button" accessibilityLabel={`Open ${sc.name} in the results widget`}
   onPress={()=>ws.openScanner(sc.id)} style={({pressed})=>[s.between,{paddingHorizontal:10,paddingVertical:9,borderRadius:9,
    backgroundColor:open.has(sc.id)?C.soft:'transparent',opacity:pressed?.7:1}]}>
   <T numberOfLines={1} style={[TYPE.body,{flex:1,color:open.has(sc.id)?C.green:C.ink}]}>{sc.name}</T>
   <T style={[TYPE.helper,{color:C.muted}]}>{sc.is_default?'KANIDA':'mine'}{sc.counts?` · ${sc.counts.active}`:''}</T>
  </Pressable>)}
 </View>;
}

export function MarketScanBody({height}:BodyProps){
 const ws=useWorkspace();
 const pick=useSelectFromTarget();
 const read=useDerivativeRead<any>(`${SCREENER_PATH}${screenerQuery([],{group:SCREENER_VIEW_DEFAULT})}`,ws.seq);
 return <WhatsHappening read={read} at={read.data?.reading_at||''} seq={ws.seq} symbol={ws.selected.underlying||''}
  onTarget={pick} table={null} showTable={false} onShowTable={NONE} style={{height:Math.max(300,height-8),borderWidth:0}}/>;
}

// =================================================================================================================
// OPTIONS
// =================================================================================================================
export function OptionChainBody({w,height}:BodyProps){
 const ws=useWorkspace();const pick=useSelectFromTarget();
 const symbol=instrumentOf(w,ws.selected),expiry=expiryOf(w,ws.selected);
 if(!symbol)return NO_SYMBOL;
 return <ChainWidget underlying={symbol} expiry={expiry} seq={ws.seq} target={null} onTarget={pick}
  highlight={focusStrike(ws,symbol)} onHighlight={ws.setStrike} style={{height:Math.max(340,height-8),borderWidth:0}}/>;
}
export function OiByStrikeBody({w,width,height,openSettings}:BodyProps){
 const ws=useWorkspace();const pick=useSelectFromTarget();
 const symbol=instrumentOf(w,ws.selected),expiry=expiryOf(w,ws.selected);
 if(!symbol)return NO_SYMBOL;
 return <OiByStrikeSection underlying={symbol} badge={symbol} linked expiry={expiry} seq={ws.seq} onTarget={pick}
  {...fit(width,height,w.settings.view,'oi_chart','oi_table')} pinFirst={width<640} onCustomize={openSettings}
  highlight={focusStrike(ws,symbol)} onHighlight={ws.setStrike}/>;
}
function useGrid(symbol:string,expiry:string,seq:number){
 return useDerivativeRead<OiGrid>(symbol?withExpiry(`/api/derivatives/oi-grid?underlying=${q(symbol)}`,expiry):null,seq);
}
export function OiSessionBody({w,width,height,openSettings}:BodyProps){
 const ws=useWorkspace();const pick=useSelectFromTarget();
 const symbol=instrumentOf(w,ws.selected),expiry=expiryOf(w,ws.selected);
 const grid=useGrid(symbol,expiry,ws.seq);
 if(!symbol)return NO_SYMBOL;
 return <OiGridSection symbol={symbol} badge={symbol} linked expiry={expiry} seq={ws.seq} read={grid} rules={[]}
  target={null} onTarget={pick} onCustomize={openSettings} {...fit(width,height,w.settings.view,'oi_grid_futures','oi_grid')}
  highlight={focusStrike(ws,symbol)} onHighlight={ws.setStrike}/>;
}
/** Owner decision Q6, the same rule the Greeks and the screener apply: implied volatility cannot be solved on the
 *  day an option expires, so an IV widget with no expiry of its own reads the NEXT expiry on expiry day — and says so. */
function useIvExpiry(symbol:string,expiry:string){
 const filters=useDerivativeRead<any>(symbol&&!expiry?'/api/derivatives/filters':null,0);
 if(expiry||!symbol)return {expiry,rolled:''};
 const list=(filters.data?.expiries||[]).filter((e:any)=>e.underlying===symbol).sort((a:any,b:any)=>a.expiry<b.expiry?-1:1);
 if(list.length>1&&list[0].days_to_expiry===0)return {expiry:list[1].expiry as string,rolled:list[0].expiry as string};
 return {expiry:'',rolled:''};
}
export function IvBody({w,width,height}:BodyProps){
 const ws=useWorkspace();const pick=useSelectFromTarget();
 const symbol=instrumentOf(w,ws.selected);
 const {expiry,rolled}=useIvExpiry(symbol,expiryOf(w,ws.selected));
 const grid=useGrid(symbol,expiry,ws.seq);
 if(!symbol)return NO_SYMBOL;
 return <View style={{gap:8}}>
  {!!rolled&&<T style={[TYPE.helper,{color:C.amber}]}>{symbol} options expiring {rolled} expire today, and implied volatility
   cannot be solved on expiry day — this reads the next expiry ({expiry}).</T>}
  <IvSection {...blockProps(symbol,expiry,ws.seq,fit(width,height-(rolled?36:0),w.settings.view,'iv_chart','iv_strikes'),pick)}
   gridRead={grid} highlight={focusStrike(ws,symbol)} onHighlight={ws.setStrike}/>
 </View>;
}
export function PcrBody({w,width,height}:BodyProps){
 const ws=useWorkspace();const pick=useSelectFromTarget();
 const symbol=instrumentOf(w,ws.selected),expiry=expiryOf(w,ws.selected);
 if(!symbol)return NO_SYMBOL;
 return <PcrSection {...blockProps(symbol,expiry,ws.seq,fit(width,height,w.settings.view,'pcr_chart','pcr_readings'),pick)}/>;
}
export function MaxPainBody({w,width,height}:BodyProps){
 const ws=useWorkspace();const pick=useSelectFromTarget();
 const symbol=instrumentOf(w,ws.selected),expiry=expiryOf(w,ws.selected);
 if(!symbol)return NO_SYMBOL;
 return <MaxPainSection {...blockProps(symbol,expiry,ws.seq,fit(width,height,w.settings.view,'max_pain_chart','max_pain_readings'),pick)}/>;
}
export function FuturesBuildupBody({w,width,height}:BodyProps){
 const ws=useWorkspace();const pick=useSelectFromTarget();
 const symbol=instrumentOf(w,ws.selected);
 if(!symbol)return NO_SYMBOL;
 return <FuturesBuildupSection {...blockProps(symbol,'',ws.seq,fit(width,height,w.settings.view,'fut_oi','fut_readings'),pick)}/>;
}

function num(v:any){return typeof v==='number'&&isFinite(v)?v:null;}
function strikeText(v:number){return v.toLocaleString('en-IN',{maximumFractionDigits:1});}
export function GreeksBody({w}:BodyProps){
 const ws=useWorkspace();
 const symbol=instrumentOf(w,ws.selected),expiry=expiryOf(w,ws.selected);
 const atm=(w.settings.atm as string)||'3';
 const read=useDerivativeRead<any>(symbol?withExpiry(`/api/workspace/greeks?underlying=${q(symbol)}&atm=${atm}`,expiry):null,ws.seq);
 if(!symbol)return NO_SYMBOL;
 const b=read.data;
 if(!b)return <Idle icon="loader" title={read.error||'Solving IV, delta and gamma…'}/>;
 if(!b.available)return <Idle icon="clock" title={b.text}/>;
 const focus=new Set((ws.selected.underlying===symbol&&ws.selected.focus?.strikes)||[]);
 const cell=(g:any,k:'iv_pct'|'delta'|'gamma')=>{const v=num(g?.[k]);return v==null?'—':k==='iv_pct'?`${v.toFixed(1)}%`:k==='delta'?v.toFixed(2):v.toFixed(4);};
 const col={width:54,textAlign:'right' as const,fontSize:11};
 return <View style={{gap:6}}>
  <View style={[s.row,{gap:6,flexWrap:'wrap'}]}><Badge label="COMPUTED" tone="amber"/>
   <T style={[TYPE.helper,{color:C.muted}]}>{b.expiry} · ATM {strikeText(b.atm_strike)} · as of {String(b.as_of).slice(11,16)}</T></View>
  {!!b.note&&<T style={[TYPE.helper,{color:C.amber}]}>{b.note}</T>}
  <View style={[s.row,{gap:4,paddingVertical:4,borderBottomWidth:1,borderColor:C.line}]}>
   <T style={[col,{color:C.muted}]}>IV</T><T style={[col,{color:C.muted}]}>Δ</T><T style={[col,{color:C.muted}]}>Γ</T>
   <T numberOfLines={1} style={[TYPE.helper,{flex:1,textAlign:'center',color:C.muted}]}>CE · K · PE</T>
   <T style={[col,{color:C.muted}]}>Γ</T><T style={[col,{color:C.muted}]}>Δ</T><T style={[col,{color:C.muted}]}>IV</T>
  </View>
  {b.rows.map((r:any)=>{const on=r.strike===b.atm_strike||focus.has(r.strike);
   return <Pressable key={r.strike} onHoverIn={()=>ws.setStrike(r.strike)} onHoverOut={()=>ws.setStrike(null)}
    style={[s.row,{gap:4,paddingVertical:5,borderRadius:6,backgroundColor:ws.strike===r.strike||focus.has(r.strike)?C.soft:'transparent'}]}>
    <T style={col}>{cell(r.CE,'iv_pct')}</T><T style={col}>{cell(r.CE,'delta')}</T><T style={col}>{cell(r.CE,'gamma')}</T>
    <T style={[TYPE.title,{flex:1,textAlign:'center',fontFamily:on?'InterSemi':'Inter',color:on?C.green:C.ink}]}>{strikeText(r.strike)}</T>
    <T style={col}>{cell(r.PE,'gamma')}</T><T style={col}>{cell(r.PE,'delta')}</T><T style={col}>{cell(r.PE,'iv_pct')}</T>
   </Pressable>;})}
  <T style={[TYPE.helper,{color:C.muted}]}>{b.computed_text} A dash is a price the model could not solve (for example, stale or at intrinsic value).</T>
 </View>;
}

export function KeyStrikesBody({w}:BodyProps){
 const ws=useWorkspace();
 const symbol=instrumentOf(w,ws.selected);
 const read=useDerivativeRead<any>(symbol?`/api/workspace/key-strikes?underlying=${q(symbol)}`:null,ws.seq);
 if(!symbol)return NO_SYMBOL;
 const b=read.data;
 if(!b)return <Idle icon="loader" title={read.error||'Reading…'}/>;
 if(!b.available)return <Idle icon="clock" title={b.text}/>;
 const ks=b.key_strikes||[];
 return <View style={{gap:6}}>
  <T style={[TYPE.helper,{color:C.muted}]}>At the {String(b.as_of).slice(11,16)} reading · {b.headline||''}</T>
  {!ks.length&&<T style={[TYPE.body,{color:C.muted}]}>No strike changed materially at this reading.</T>}
  {ks.map((k:any)=><Pressable key={`${k.strike}${k.side}`} onHoverIn={()=>ws.setStrike(k.strike)} onHoverOut={()=>ws.setStrike(null)}
   style={[s.between,{paddingHorizontal:10,paddingVertical:8,borderRadius:9,borderWidth:1,borderColor:k.lead?C.green:C.line,
    backgroundColor:ws.strike===k.strike?C.soft:C.dark}]}>
   <T style={[TYPE.title,{color:k.side==='CE'?C.green:C.red}]}>{strikeText(k.strike)} {k.side}</T>
   <T style={[TYPE.helper,{color:k.lead?C.green:C.muted}]}>{k.note}</T>
  </Pressable>)}
 </View>;
}

// =================================================================================================================
// CHARTS
// =================================================================================================================
export function PriceChartBody({w,height}:BodyProps){
 const ws=useWorkspace();
 const symbol=instrumentOf(w,ws.selected);
 if(!symbol)return NO_SYMBOL;
 return <FuturesChartPanel underlying={symbol} seq={ws.seq} style={{height:Math.max(320,height-8),borderWidth:0}}/>;
}
export function VolumeBody({w,width,height}:BodyProps){
 const ws=useWorkspace();
 const symbol=instrumentOf(w,ws.selected),expiry=expiryOf(w,ws.selected);
 const read=useDerivativeRead<any>(symbol?withExpiry(`/api/workspace/volume?underlying=${q(symbol)}`,expiry):null,ws.seq);
 if(!symbol)return NO_SYMBOL;
 const b=read.data;
 if(!b)return <Idle icon="loader" title={read.error||'Reading volume…'}/>;
 if(!b.available)return <Idle icon="clock" title={b.text}/>;
 const pts=(b.points||[]).filter((p:any)=>!p.opening);
 const max=Math.max(1,...pts.map((p:any)=>Math.max(p.ce||0,p.pe||0)));
 const W=Math.max(200,width-40),H=Math.max(120,height-150),bw=Math.max(2,W/Math.max(1,pts.length)/2.6);
 const fmt=(v:number)=>v>=1e7?`${(v/1e7).toFixed(1)}Cr`:v>=1e5?`${(v/1e5).toFixed(1)}L`:`${Math.round(v/1e3)}K`;
 return <View style={{gap:6}}>
  <T style={[TYPE.helper,{color:C.muted}]}>{b.expiry} · per 15-min interval · <T style={[TYPE.helper,{color:C.green}]}>calls</T> / <T style={[TYPE.helper,{color:C.red}]}>puts</T></T>
  <Svg width={W} height={H}>
   <SvgLine x1={0} y1={H-1} x2={W} y2={H-1} stroke={C.line}/>
   {pts.map((p:any,i:number)=>{const x=(i+.5)*W/pts.length;
    return <React.Fragment key={p.at}>{p.gap?null:<>
     <Rect x={x-bw-1} y={H-(p.ce||0)/max*(H-4)} width={bw} height={(p.ce||0)/max*(H-4)} fill={C.green} opacity={.85}/>
     <Rect x={x+1} y={H-(p.pe||0)/max*(H-4)} width={bw} height={(p.pe||0)/max*(H-4)} fill={C.red} opacity={.85}/></>}
    </React.Fragment>;})}
  </Svg>
  <View style={s.between}><T style={[TYPE.helper,{color:C.muted}]}>{pts[0]?.at}</T><T style={[TYPE.helper,{color:C.muted}]}>peak {fmt(max)}</T><T style={[TYPE.helper,{color:C.muted}]}>{pts[pts.length-1]?.at}</T></View>
  <T style={[TYPE.helper,{color:C.muted}]}>{b.definition}</T>
 </View>;
}

// =================================================================================================================
// INTELLIGENCE
// =================================================================================================================
/** The user's scanner beside the engine — the same block the AI summary opens with (alignment.py). */
function ScannerStrip({symbol}:{symbol:string}){
 const ws=useWorkspace();
 const f:any=ws.selected.underlying===symbol?ws.selected.focus:null;
 const read=useDerivativeRead<any>(f?.scanner_id?`/api/workspace/scanner-context?underlying=${q(symbol)}&scanner_id=${q(f.scanner_id)}&match_key=${q(f.match_key||'')}`:null,ws.seq);
 const ctx=read.data?.context;
 if(!ctx)return null;
 return <View style={{gap:4,padding:10,borderRadius:10,borderWidth:1,borderColor:'#1F4D3E',backgroundColor:C.soft,marginBottom:8}}>
  <T style={[TYPE.label,{color:C.green}]}>{ctx.mine?'YOUR SCANNER':'KANIDA SCANNER'} · {ctx.scanner}</T>
  <T style={TYPE.body}>{ctx.status_text}</T>
  {(ctx.agreement||[]).map((a:any,i:number)=><View key={i} style={[s.row,{gap:6,alignItems:'flex-start'}]}>
   <Icon name={a.agrees===true?'check-circle':a.agrees===false?'git-branch':'minus-circle'} size={12} color={a.agrees===true?C.green:a.agrees===false?C.amber:C.muted}/>
   <T style={[TYPE.helper,{flex:1,color:a.agrees===false?C.amber:C.ink}]}>{a.text}</T></View>)}
 </View>;
}
export function SignalBody({w,height}:BodyProps){
 const ws=useWorkspace();
 const symbol=instrumentOf(w,ws.selected),expiry=expiryOf(w,ws.selected);
 const grid=useGrid(symbol,expiry,ws.seq);
 const path=(p:string)=>symbol?withExpiry(`/api/derivatives/${p}?underlying=${q(symbol)}`,expiry):null;
 const pcr=useDerivativeRead<any>(path('pcr-series'),ws.seq),mp=useDerivativeRead<any>(path('maxpain-series'),ws.seq);
 const iv=useDerivativeRead<any>(path('iv-series'),ws.seq),stand=useDerivativeRead<any>(path('oi-by-strike'),ws.seq);
 if(!symbol)return NO_SYMBOL;
 return <View style={{flex:1}}>
  <ScannerStrip symbol={symbol}/>
  <SignalPanel symbol={symbol} read={grid} pcr={pcr.data} maxPain={mp.data} iv={iv.data} standing={stand.data}
   style={{height:Math.max(360,height-8),width:'100%',borderWidth:0}}/>
 </View>;
}
export function SessionHistoryBody({w,height}:BodyProps){
 const ws=useWorkspace();
 const symbol=instrumentOf(w,ws.selected),expiry=expiryOf(w,ws.selected);
 const grid=useGrid(symbol,expiry,ws.seq);
 if(!symbol)return NO_SYMBOL;
 return <SignalTable symbol={symbol} read={grid} style={{height:Math.max(320,height-8),borderWidth:0}}/>;
}

const SECTION_TITLE:Record<string,string>={scanner:'Your scanner',context:'Why this instrument',changed:'What changed',where:'Where',
 persistent:'Persistent',conflicting:'Conflicting',key_strikes:'Key strikes',unusual:'Unusual'};
export function AiSummaryBody({w}:BodyProps){
 const ws=useWorkspace();
 const symbol=instrumentOf(w,ws.selected);
 const scope=(w.settings.scope as string)||'connected';
 // the widgets reading THIS instrument are the summary's sources — follow or pinned to the same symbol
 const sources=useMemo(()=>Array.from(new Set(ws.widgets.filter(x=>x.widget_id!==w.widget_id&&instrumentOf(x,ws.selected)===symbol)
  .map(x=>x.widget_type))).sort(),[ws.widgets,ws.selected,symbol,w.widget_id]);
 const focus=ws.selected.underlying===symbol?ws.selected.focus:null;
 const [body,setBody]=useState<Summary|null>(null);
 const [err,setErr]=useState('');
 const key=JSON.stringify([symbol,sources,scope,focus,ws.seq]);
 useEffect(()=>{if(!symbol)return;let live=true;setErr('');
  wsApi.summary({underlying:symbol,sources,scope,focus}).then(b=>{if(live)setBody(b)},e=>{if(live)setErr(e?.message||'')});
  return ()=>{live=false}},[key]);  // eslint-disable-line react-hooks/exhaustive-deps
 if(!symbol)return NO_SYMBOL;
 if(err)return <Idle icon="alert-circle" title="The summary could not be read" detail={err}/>;
 if(!body||body.underlying!==symbol)return <Idle icon="loader" title="Reading the connected widgets…"/>;
 if(!body.available)return <Idle icon="clock" title={body.text||''}/>;
 const order=['scanner','context','changed','where','persistent','conflicting','key_strikes','unusual'];
 return <View style={{gap:12}}>
  <View style={[s.row,{gap:6,flexWrap:'wrap'}]}>
   <Badge label="AI SUMMARY · RULES, NOT A MODEL" tone="neutral"/>
   <T style={[TYPE.helper,{color:C.muted}]}>{symbol} · {body.expiry} · the {String(body.as_of).slice(11,16)} reading</T>
  </View>
  {order.map(k=>{const lines=(body.sections as any)?.[k]||[];if(!lines.length)return null;
   return <View key={k} style={{gap:4}}>
    <T style={[TYPE.label,{fontSize:10,color:k==='conflicting'?C.amber:k==='scanner'?C.green:C.muted}]}>
     {k==='scanner'?((body as any).scanner?.mine?'Your scanner':'KANIDA scanner'):SECTION_TITLE[k]}</T>
    {lines.map((l:any,i:number)=><View key={i} style={[s.row,{gap:8,alignItems:'flex-start'}]}>
     <T style={[TYPE.body,{flex:1}]}>{l.text}</T>
     <T style={[TYPE.helper,{color:C.muted,paddingTop:4}]}>{l.source}</T></View>)}
   </View>;})}
  <T style={[TYPE.helper,{color:C.muted}]}>Read from: {(body.read_from||[]).join(', ')}.
   {body.not_read?.length?` Not read (no widget on this workspace): ${body.not_read.join(', ')} — add one, or set the summary to read all sources.`:''} {body.caveat}</T>
 </View>;
}

export function SignalNoiseBody(){
 const ws=useWorkspace();
 const read=useDerivativeRead<any>('/api/derivatives/signal-noise',ws.seq);
 const b=read.data;
 if(!b)return <Idle icon="loader" title={read.error||'Reading the audit…'}/>;
 if(!b.insights)return <Idle icon="clock" title="No readings audited for this session yet"/>;
 const pct=(v:any)=>typeof v==='number'?`${Math.round(v*100)}%`:'—';
 const f=b.final||{};
 const stat=(label:string,value:string,note?:string)=><View style={{flex:1,minWidth:110,gap:2}}>
  <T style={[TYPE.helper,{color:C.muted}]}>{label}</T><T style={TYPE.metric}>{value}</T>
  {!!note&&<T style={[TYPE.helper,{color:C.muted}]}>{note}</T>}</View>;
 return <View style={{gap:10}}>
  <T style={[TYPE.helper,{color:C.muted}]}>Session {b.session} · {b.insights} readings recorded · {b.sn_version}</T>
  <View style={[s.row,{gap:10,flexWrap:'wrap'}]}>
   {stat('Signal',String(f.signal||0))}{stat('Noise',String(f.noise||0))}{stat('Pending',String(f.pending||0))}
   {stat('Confirmed at 15 min',pct(b.confirmed_15m_rate),`baseline ${pct(b.baseline_15m_rate)}`)}
  </View>
  <T style={[TYPE.helper,{color:C.muted}]}>An audit of what the 15-min readings said against what followed them. It grades the readings, not a trade.</T>
 </View>;
}

// =================================================================================================================
// MARKET — lists that point the workspace at an instrument
// =================================================================================================================
export function IndexDashboardBody({height}:BodyProps){
 const ws=useWorkspace();const pick=useSelectFromTarget();
 return <IndexWidget seq={ws.seq} target={null} onTarget={pick} style={{height:Math.max(320,height-8),borderWidth:0}}/>;
}
export function FuturesListBody({height}:BodyProps){
 const ws=useWorkspace();const pick=useSelectFromTarget();
 return <FuturesWidget underlying="" watchlist="all" seq={ws.seq} target={null} onTarget={pick} style={{height:Math.max(320,height-8),borderWidth:0}}/>;
}
export function WatchlistBody(){
 const ws=useWorkspace();
 const prod=useDerivativeRead<any>('/api/product',ws.seq);
 const vocab=useDerivativeRead<any>('/api/screener/vocabulary',0);
 const fno=new Set<string>(vocab.data?.underlyings||[]);
 const items:any[]=prod.data?.watchlist||[];
 if(!prod.data)return <Idle icon="loader" title="Reading your watchlist…"/>;
 if(!items.length)return <Idle icon="eye" title="Your watchlist is empty" detail="Add setups to watch from Discover or the chart."/>;
 return <View style={{gap:4}}>
  {items.map((it:any)=>{const sym=String(it.symbol||'').toUpperCase();const ok=fno.has(sym);
   return <Pressable key={it.id} disabled={!ok} accessibilityRole="button" onPress={()=>ws.select({underlying:sym,expiry:null,focus:null})}
    style={({pressed})=>[s.between,{paddingHorizontal:10,paddingVertical:8,borderRadius:9,opacity:!ok?.5:pressed?.7:1,
     backgroundColor:ws.selected.underlying===sym?C.soft:'transparent'}]}>
    <T style={TYPE.title}>{sym}</T>
    <T style={[TYPE.helper,{color:C.muted}]}>{it.pattern_name||it.pattern} · {it.timeframe}{ok?'':' · no F&O'}</T>
   </Pressable>;})}
 </View>;
}
export function AlertsBody(){
 const ws=useWorkspace();
 const read=useDerivativeRead<any>(`/api/screener/alerts?seq=${ws.seq}`,ws.seq);
 const items:any[]=read.data?.alerts||[];
 if(!read.data)return <Idle icon="loader" title="Reading alerts…"/>;
 if(!items.length)return <Idle icon="bell" title="No alerts yet" detail={'Turn on "Notify me when…" on any scanner. Only changes are sent.'}/>;
 return <View style={{gap:6}}>
  {items.map(a=>{const und=String(a.entity_key||'').split('|')[0];
   return <Pressable key={a.id} onPress={()=>und&&ws.select({underlying:und,expiry:String(a.entity_key).split('|')[1]||null,focus:null})}
    style={({pressed})=>({gap:3,padding:9,borderRadius:9,borderWidth:1,borderColor:C.line,opacity:a.read?(pressed?.5:.7):(pressed?.7:1)})}>
    <View style={[s.row,{gap:6}]}><Badge label={a.kind==='new'?'NEW':a.kind==='ended'?'ENDED':'CHANGED'} tone={a.kind==='ended'?'neutral':a.kind==='changed'?'amber':'green'}/>
     <T style={[TYPE.helper,{color:C.muted}]}>{String(a.reading_at).slice(11,16)} · {a.scanner_name}</T></View>
    <T style={TYPE.title}>{a.title}</T>
    <T numberOfLines={2} style={[TYPE.helper,{color:C.muted}]}>{a.text}</T>
   </Pressable>;})}
 </View>;
}

/** widget type → body. A type the page does not know renders nothing rather than a guess. */
export const BODIES:Record<string,(p:BodyProps)=>React.ReactElement|null>={
 screener_results:ScreenerResultsBody,screener_builder:ScreenerBuilderBody,scanners:ScannersBody,market_scan:MarketScanBody,
 option_chain:OptionChainBody,oi_by_strike:OiByStrikeBody,oi_session:OiSessionBody,iv:IvBody,pcr:PcrBody,max_pain:MaxPainBody,
 greeks:GreeksBody,key_strikes:KeyStrikesBody,price_chart:PriceChartBody,volume:VolumeBody,signal:SignalBody,
 ai_summary:AiSummaryBody,session_history:SessionHistoryBody,signal_noise:SignalNoiseBody,futures_buildup:FuturesBuildupBody,
 index_dashboard:IndexDashboardBody,futures_list:FuturesListBody,watchlist:WatchlistBody,alerts:AlertsBody,
};
/** Widgets that render the Derivative tab's own full block: their body scrolls inside the tile. */
export const SELF_SCROLLING=new Set(['option_chain','market_scan','price_chart','signal','session_history','index_dashboard','futures_list']);
/** Blocks laid out to FIT their tile: rendered in a frame that never scrolls (fit() above sizes them to it). */
export const FIT=new Set(['pcr','max_pain','iv','futures_buildup','oi_by_strike','oi_session']);
