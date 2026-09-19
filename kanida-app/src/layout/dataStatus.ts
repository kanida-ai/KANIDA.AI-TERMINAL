// Data provenance + freshness, as plain data. Pure: no React, no react-native, no fetching —
// so scripts/check-data-status.cjs can run it directly and the Popover panel stays dumb.
//
// Input is `/api/state.data_status` (market_scanner/data_status.py), which the pilot proxies
// untouched. Every timestamp it carries is naive IST; the browser may be in any zone, so every
// comparison here converts IST -> epoch explicitly rather than trusting `new Date(text)`.
//
// The one rule worth stating out loud: **a vendor delay is not staleness**. `market_data`'s
// 15-minute vendor publishes with delay_seconds=900 by design (contract §5). Subtracting that
// before judging "behind" is what keeps the pill green after a vendor swap.
import {DATA_STALE_DAYS} from '../constants';

export type DataStatusRunInfo={run_id?:string|null;started_at?:string|null;finished_at?:string|null;status?:string|null;provider?:string|null;symbols?:number|null;requests?:number|null;bars_written?:number|null;daily_bars_written?:number|null;errors?:number|null;seconds?:number|null;running?:boolean|null;age_seconds?:number|null;started_age_seconds?:number|null;last_success_at?:string|null;last_success_age_seconds?:number|null;note?:string|null};
export type DataStatus={
 version?:number;as_of?:string|null;timezone?:string|null;
 provider?:{id?:string|null;label?:string|null;delay_seconds?:number|null;note?:string|null}|null;
 source?:{kind?:string|null;label?:string|null;live?:boolean|null;store?:string|null;note?:string|null}|null;
 latest_bar?:{start?:string|null;end?:string|null;by_timeframe?:Record<string,string|null>|null;note?:string|null}|null;
 patterns?:DataStatusPatterns|null;
 last_run?:DataStatusRunInfo|null;last_run_note?:string|null;
 next_refresh?:{at?:string|null;in_seconds?:number|null;note?:string|null}|null;
 session?:{state?:string|null;reason?:string|null;date?:string|null;open?:string|null;close?:string|null;next_open?:string|null;note?:string|null}|null;
 stalled?:{value?:boolean|null;after_minutes?:number|null;note?:string|null}|null;
 stale?:{value?:boolean|null;note?:string|null}|null;
 error?:string|null;note?:string|null;
};
// What the pattern scan has covered (market_scanner/data_status.py patterns_block). Prices move every
// 15 minutes; patterns only when a candle completes, so the two clocks are reported separately.
export type PatternFrame={scanned?:string|null;scanned_at?:string|null;expected?:string|null;next_close?:string|null;due?:boolean|null;behind?:boolean|null;scanning?:boolean|null;overdue_seconds?:number|null};
export type DataStatusPatterns={latest?:string|null;next_update?:string|null;up_to_date?:boolean|null;updating?:string[]|null;behind?:string[]|null;by_timeframe?:Record<string,PatternFrame>|null;error?:string|null;note?:string|null};
export type DataStatusTone='fresh'|'stale'|'very-stale'|'unknown';
export type DataStatusRow={key:string;label:string;value:string;note?:string;tone?:DataStatusTone};
export type DataStatusView={
 tone:DataStatusTone;live:boolean;kind:string;headline:string;detail:string;
 rows:DataStatusRow[];warning:{tone:DataStatusTone;text:string}|null;expectation:string;a11y:string;
};

const MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const IST_OFFSET_MS=330*6e4;
const STAMP=/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?/;

// A naive-IST stamp as epoch ms, so it can be compared with Date.now() from any browser zone.
export function istMs(stamp?:string|null):number|undefined{
 const m=STAMP.exec(String(stamp||''));if(!m)return undefined;
 return Date.UTC(+m[1],+m[2]-1,+m[3],+(m[4]||0),+(m[5]||0),+(m[6]||0))-IST_OFFSET_MS;
}
// "16 Sep 09:30" (time dropped when the stamp is date-only). Year appended only when it is not `now`'s.
export function formatIst(stamp?:string|null,now:Date=new Date()):string{
 const m=STAMP.exec(String(stamp||''));if(!m)return '—';
 const ist=new Date(now.getTime()+IST_OFFSET_MS);
 const year=+m[1]===ist.getUTCFullYear()?'':` ${m[1]}`;
 const day=`${+m[3]} ${MONTHS[+m[2]-1]}${year}`;
 return m[4]?`${day} ${m[4]}:${m[5]}`:day;
}
// "just now" / "12 min ago" / "3h 5m ago" / "4 days ago". Negative (a future stamp) reads as "just now".
export function agoText(seconds?:number|null):string{
 if(seconds==null||!Number.isFinite(seconds))return '—';
 const s=Math.max(0,Math.round(seconds));
 if(s<90)return 'just now';
 const mins=Math.round(s/60);if(mins<60)return `${mins} min ago`;
 const hours=Math.floor(mins/60),rest=mins%60;
 if(hours<24)return rest?`${hours}h ${rest}m ago`:`${hours}h ago`;
 const days=Math.round(hours/24);return `${days} day${days===1?'':'s'} ago`;
}
export function inText(seconds?:number|null):string{
 if(seconds==null||!Number.isFinite(seconds))return '';
 const s=Math.max(0,Math.round(seconds));
 if(s<60)return 'in under a minute';
 const mins=Math.round(s/60);if(mins<60)return `in ${mins} min`;
 const hours=Math.floor(mins/60),rest=mins%60;
 if(hours<24)return rest?`in ${hours}h ${rest}m`:`in ${hours}h`;
 return `in ${Math.round(hours/24)} days`;
}
export const SESSION_TEXT:Record<string,string>={pre_open:'Pre-open',open:'Open',closed:'Closed',holiday:'Closed'};

// Seconds the newest bar is behind the clock IN EXCESS of the vendor's published delay.
// undefined when we cannot tell. A delayed vendor sitting exactly on its delay returns 0.
export function barLagSeconds(status?:DataStatus|null,now:Date=new Date()):number|undefined{
 const end=istMs(status?.latest_bar?.end||status?.latest_bar?.start);if(end==null)return undefined;
 const delay=Number(status?.provider?.delay_seconds)||0;
 return Math.max(0,Math.round((now.getTime()-end)/1000)-delay);
}
export const stallLimitMinutes=(status?:DataStatus|null)=>{
 const v=Number(status?.stalled?.after_minutes);return Number.isFinite(v)&&v>0?v:20;
};
const BAR_SECONDS=15*60;
// Market open and the newest bar more than one bar + the stall limit behind (after the vendor delay).
// The next bar is only due one bar after the newest one ends, and the loop then needs a cycle to write
// it, so "one bar + 20 min" is a missed bar, never the normal wait for the next one.
export function pricesLagging(status?:DataStatus|null,now:Date=new Date()):boolean{
 if(status?.source?.live!==true||status?.session?.state!=='open')return false;
 const lag=barLagSeconds(status,now);return lag!=null&&lag>BAR_SECONDS+stallLimitMinutes(status)*60;
}

// Red / amber / green, unchanged for the historical source (calendar age vs DATA_STALE_DAYS) and
// extended for the live one: stalled is red; the server's own `stale` flag or a newest bar more
// than the stall limit behind (AFTER the vendor delay) is amber.
export function dataStatusTone(status:DataStatus|null|undefined,ageDays:number|undefined,staleLimit:number=DATA_STALE_DAYS,now:Date=new Date()):DataStatusTone{
 const redAfter=staleLimit*5;
 const base:DataStatusTone=ageDays==null||!Number.isFinite(ageDays)?'unknown':ageDays>redAfter?'very-stale':ageDays>staleLimit?'stale':'fresh';
 if(!status||status.error)return base;
 if(status.stalled?.value===true)return 'very-stale';
 // A live source is judged by the feed itself, never by calendar days: after a long weekend the
 // newest bar is 4 days old and perfectly current.
 const live=status.source?.live===true;
 if(!live&&base==='very-stale')return base;
 if(status.stale?.value===true)return 'stale';
 if(pricesLagging(status,now))return 'stale';
 return live?'fresh':base;
}

export type DataPillContent={text:string;label:string;tone:DataStatusTone;stale:boolean;line:string};

// --- the scanner connection (BACKLOG item 2a) ----------------------------------------------------------------
// The scanner takes minutes to load its scan at startup and can be restarted mid-day. The pilot replays the
// last successful body with this provenance attached (server/kanida_pilot/evidence.py `provenance`), so the app
// can keep SHOWING that scan and say when it was taken, instead of a full-page error.
export type CacheProvenance={served_from_cache?:boolean|null;cached_at?:string|null;cached_as_of?:string|null;
 upstream_error?:string|null;upstream_down_seconds?:number|null;reconnecting?:boolean|null;unreachable?:boolean|null};
/** Matches RECONNECT_GRACE_SECONDS in server/kanida_pilot/evidence.py: below this it is "reconnecting". */
export const RECONNECT_GRACE_SECONDS=300;
export type ConnectionMode='reconnecting'|'lost'|'offline';
export type ConnectionView={mode:ConnectionMode;tone:DataStatusTone;
 /** The one line shown under the top bar and on the Discover page. */
 text:string;
 /** The same fact in a pill's worth of room. */
 short:string;
 /** Why, in plain words. */
 detail:string;retry:boolean;a11y:string};
// "8 min" / "2h 5m" — how long the scanner has been silent, without the "ago" that `agoText` adds.
function forText(seconds:number):string{
 const s=Math.max(0,Math.round(seconds));
 if(s<90)return 'under a minute';
 const mins=Math.round(s/60);if(mins<60)return `${mins} min`;
 const hours=Math.floor(mins/60),rest=mins%60;
 if(hours<24)return rest?`${hours}h ${rest}m`:`${hours}h`;
 const days=Math.round(hours/24);return `${days} day${days===1?'':'s'}`;
}
/** What to say about the scanner, or null when it is answering normally.
 *
 * Three cases, and only the last is an error:
 *  - a scan IS being shown and the scanner has been quiet for under 5 minutes → "reconnecting", quietly;
 *  - a scan IS being shown and it has been longer → same data, plainly told the connection has not come back;
 *  - nothing was ever cached and the request failed → there is genuinely nothing to show. */
export function connectionView(cache?:CacheProvenance|null,opts:{error?:string;now?:Date}={}):ConnectionView|null{
 const now=opts.now||new Date();
 const raw=Number(cache?.upstream_down_seconds);
 const down=Number.isFinite(raw)?Math.max(0,raw):0;
 const why=String(cache?.upstream_error||'').trim();
 if(cache?.served_from_cache===true){
  const when=formatIst(cache?.cached_at,now);
  const lost=cache?.reconnecting===false||down>RECONNECT_GRACE_SECONDS;
  const text=lost?`Showing the last scan · ${when} · not reconnected for ${forText(down)}`
   :`Showing the last scan · ${when} · reconnecting…`;
  const detail=lost
   ?`${why||'The scanner is not answering.'} Nothing here has changed since ${when}.`
   :`${why||'The scanner is not answering.'} The page keeps showing the last scan while it comes back.`;
  return {mode:lost?'lost':'reconnecting',tone:lost?'very-stale':'stale',text,
   short:lost?`Last scan ${clockText(cache?.cached_at,now)}`:`Last scan ${clockText(cache?.cached_at,now)} · reconnecting`,
   detail,retry:true,a11y:`${text}. ${detail}`};
 }
 if(opts.error||cache?.unreachable===true){
  const detail=why||opts.error||'The scanner is not answering.';
  const text='The scanner is not answering and there is no earlier scan to show.';
  return {mode:'offline',tone:'very-stale',text,short:'No scan',detail,retry:true,a11y:`${text} ${detail}`};
 }
 return null;
}

// "09:45" when the stamp is on today's IST date, else "16 Sep 15:30".
export function clockText(stamp?:string|null,now:Date=new Date()):string{
 const m=STAMP.exec(String(stamp||''));if(!m)return '—';
 const ist=new Date(now.getTime()+IST_OFFSET_MS);
 const today=+m[1]===ist.getUTCFullYear()&&+m[2]===ist.getUTCMonth()+1&&+m[3]===ist.getUTCDate();
 return today&&m[4]?`${m[4]}:${m[5]}`:formatIst(stamp,now);
}
const lower=(t:string)=>t?t[0].toLowerCase()+t.slice(1):t;
const joinAnd=(xs:string[])=>xs.length<2?xs.join(''):`${xs.slice(0,-1).join(', ')} and ${xs[xs.length-1]}`;

// The one story the pill, the panel and the Discover header all tell about a LIVE source: two plain
// facts (when prices last arrived, which candle the pattern scan last covered) plus when patterns
// next update. Anything the scanner did not report is named in `missing`, never guessed.
export type DataStory={
 open:boolean;tone:DataStatusTone;
 prices:string;patterns:string;next:string;line:string;pill:string;compact:string;label:string;
 pricesBehind:string|null;patternsBehind:string|null;missing:string[];
};
export function dataStory(status:DataStatus,now:Date=new Date(),ageDays?:number,staleLimit:number=DATA_STALE_DAYS):DataStory{
 const open=status.session?.state==='open'||!status.session?.state;
 const bar=status.latest_bar?.end||status.latest_bar?.start||'';
 const delay=Number(status.provider?.delay_seconds)||0,lag=barLagSeconds(status,now);
 const p=status.patterns,frames=p?.by_timeframe||{},missing:string[]=[];
 // Older scanners have no `patterns` block; latest_bar.by_timeframe is the same scanned candle per timeframe.
 const scanned=[...Object.values(frames).map(f=>f?.scanned),...Object.values(status.latest_bar?.by_timeframe||{})].filter((v):v is string=>!!v);
 const latest=p?.latest||(scanned.length?scanned.reduce((a,b)=>a>b?a:b):'');
 const stalled=status.stalled?.value===true,stale=status.stale?.value===true,lagging=pricesLagging(status,now);
 if(!bar)missing.push('the newest price bar');
 if(!latest)missing.push('the newest candle the pattern scan covered');
 if(!p||p.error)missing.push('when patterns next update');

 const pricesBehind=stalled?`The price feed looks stalled. ${status.stalled?.note||`No price refresh has finished in the last ${stallLimitMinutes(status)} minutes while the market is open.`}`
  :stale?`Prices are behind the market. ${status.stale?.note||'The newest bar is behind the newest completed session.'}`
  :lagging?`The market is open but the newest price bar is ${agoText((lag||0)+delay)}${delay?' (the vendor delay is already allowed for)':''}.`:null;
 const behind=(p?.behind||[]).filter(Boolean),updating=(p?.updating||[]).filter(Boolean);
 const patternsBehind=behind.length?`Patterns are behind: ${joinAnd(behind.map(tf=>{const f=frames[tf]||{};
   return f.expected?`the ${tf} candle that closed ${clockText(f.expected,now)}${f.overdue_seconds!=null?` (${agoText(f.overdue_seconds)})`:''}`:`the ${tf} candle`;}))} ${behind.length===1?'has':'have'} not been scanned yet.`:null;

 const priceWord=stalled?'STALLED':(stale||lagging)?'BEHIND':'';
 const state=behind.length?'behind':updating.length?'updating':p?.up_to_date===true?'current':'unknown';
 const prices=bar?`Prices ${open?clockText(bar,now):`to ${formatIst(bar,now)}`}`:'Prices: newest bar unknown';
 const patterns=state==='behind'?`Patterns BEHIND${latest?` · last updated ${clockText(latest,now)}`:''}`
  :!open&&state==='current'?'Patterns up to date'
  :!open&&state==='updating'?'Patterns updating'
  :latest?`Patterns updated ${clockText(latest,now)}`:'Patterns: last scan unknown';
 const next=state==='behind'?'':state==='updating'?'updating now':p?.next_update?`next update ${clockText(p.next_update,now)}`:'';

 const line=open
  ?[`${prices}${priceWord?` (${priceWord.toLowerCase()})`:''}`,lower(patterns),next].filter(Boolean).join(' · ')
  :['Market closed',lower(prices),lower(patterns)].join(' · ');
 const pillPatterns=state==='behind'?'Patterns BEHIND':open?(latest?`Patterns ${clockText(latest,now)}`:'Patterns ?'):patterns;
 const pill=[open?'Live':'Closed',priceWord,bar?`Prices ${open?clockText(bar,now):formatIst(bar,now)}`:'Prices ?',pillPatterns].filter(Boolean).join(' · ');
 const compact=stalled?'STALLED':(stale||lagging||behind.length)?'BEHIND':open?(bar?`Live ${clockText(bar,now)}`:'Live ?'):'Closed';
 const provider=status.provider?.label||status.provider?.id||'the live feed';
 const label=`Market data ${open?'live':'closed'} from ${provider}: prices ${bar?`${formatIst(bar,now)} IST`:'unknown'}, ${lower(patterns)}${next?`, ${next}`:''}`
  +(stalled?', ingestion stalled':(stale||lagging)?', prices behind the market':'')+(behind.length?', patterns behind':'');
 let tone=dataStatusTone(status,ageDays,staleLimit,now);
 if(tone==='fresh'&&behind.length)tone='stale';
 return {open,tone,prices,patterns,next,line,pill,compact,label,pricesBehind,patternsBehind,missing};
}
// What the pill itself says. With no `data_status` (an older scanner, or the Discover catalog's
// own date) this returns byte-for-byte what the pill said before this popover existed; with a
// live source it names the source and the bar time instead of a meaningless "0d".
export function pillContent(status:DataStatus|null|undefined,opts:{dataEnd?:string;ageDays?:number;staleLimit?:number;compact?:boolean;now?:Date;cache?:CacheProvenance|null;error?:string}={}):DataPillContent{
 const now=opts.now||new Date(),staleLimit=opts.staleLimit??DATA_STALE_DAYS,compact=!!opts.compact;
 // A scan is being shown but the scanner is quiet: say WHEN that scan was taken. "Data age unknown" would
 // be both unhelpful and wrong — the age is known exactly, it is just not moving.
 const conn=connectionView(opts.cache,{error:opts.error,now});
 if(conn)return {text:compact?conn.short:conn.text,label:conn.a11y,tone:conn.tone,stale:true,line:conn.text};
 const age=opts.ageDays,date=formatDataDate(opts.dataEnd);
 const tone=dataStatusTone(status,age,staleLimit,now);
 const stale=tone==='stale'||tone==='very-stale';
 const live=!!status&&!status.error&&status.source?.live===true;
 if(live){
  const story=dataStory(status!,now,age,staleLimit);
  return {text:compact?story.compact:story.pill,label:story.label,tone:story.tone,
   stale:story.tone==='stale'||story.tone==='very-stale',line:story.line};
 }
 const text=age==null?(compact?'Data ?':date?`Data: ${date} · age unknown`:'Data age unknown')
  :compact?`${stale?'STALE ':''}${age}d`:`Data: ${date?`${date} · `:''}${stale?'STALE ':''}${age}d`;
 const label=age==null?`Market data age unknown${date?`, data ends ${date}`:''}`
  :`Market data ${date?`ends ${date}, `:''}${age} day${age===1?'':'s'} old${stale?`, stale, limit ${staleLimit} days`:''}`;
 const line=age==null?(date?`Data to ${date} · age unknown`:'Data age unknown')
  :`Data to ${date||'an unknown date'} · ${stale?'STALE · ':''}${age} day${age===1?'':'s'} old`;
 return {text,label,tone,stale,line};
}
// The short amber line a Discover card shows, or null when there is nothing to warn about. With a live
// source only a genuinely behind feed or scan warns (never calendar days); the stored research source
// keeps its "N days old" line because those prices really are old.
export function cardWarning(status:DataStatus|null|undefined,opts:{stale?:boolean;ageDays?:number|null;now?:Date}={}):string|null{
 const now=opts.now||new Date();
 if(status&&!status.error&&status.source?.live===true){
  const st=dataStory(status,now,opts.ageDays??undefined);
  const bar=status.latest_bar?.end||status.latest_bar?.start;
  if(st.pricesBehind)return status.stalled?.value===true?`Price feed stalled${bar?` · newest prices ${clockText(bar,now)}`:''}`:`Prices behind${bar?` · newest ${clockText(bar,now)}`:''}`;
  if(st.patternsBehind)return `Patterns behind${status.patterns?.latest?` · last updated ${clockText(status.patterns.latest,now)}`:''}`;
  return null;
 }
 if(!opts.stale)return null;
 return `Research only — prices ${opts.ageDays!=null?`${opts.ageDays} days old`:'of unknown age'}`;
}
// A chart card's footer: the candle this pattern was read on, next to the newest price time, in the same
// words as the header line. Without a live source it stays "Data to 16 Sep".
export function cardDataLine(status:DataStatus|null|undefined,dataEnd?:string|null,now:Date=new Date()):string{
 if(status&&!status.error&&status.source?.live===true){
  const bar=status.latest_bar?.end||status.latest_bar?.start;
  return [`Pattern as of ${dataEnd?clockText(dataEnd,now):'an unknown candle'}`,bar?`prices ${clockText(bar,now)}`:'newest prices unknown'].join(' · ');
 }
 return `Data to ${dataEnd?formatIst(dataEnd,now):'unknown date'}`;
}
// Calendar days between a YYYY-MM-DD data end and today's IST market date; undefined when unparseable.
export function dataAgeDays(dataEnd?:string,now:Date=new Date()):number|undefined{
 const m=STAMP.exec(String(dataEnd||''));if(!m)return undefined;
 const end=Date.UTC(+m[1],+m[2]-1,+m[3]);const ist=new Date(now.getTime()+IST_OFFSET_MS);
 const today=Date.UTC(ist.getUTCFullYear(),ist.getUTCMonth(),ist.getUTCDate());
 return Math.max(0,Math.round((today-end)/864e5));
}
export function formatDataDate(dataEnd?:string):string|undefined{
 const m=STAMP.exec(String(dataEnd||''));return m?`${+m[3]} ${MONTHS[+m[2]-1]} ${m[1]}`:dataEnd||undefined;
}

function runRow(status:DataStatus):DataStatusRow{
 const run=status.last_run;
 if(!run)return {key:'refresh',label:'Last price refresh',value:'Never',note:status.last_run_note||undefined,tone:status.source?.live?'stale':undefined};
 const bits:string[]=[];
 // A cycle in flight is the loop working: say so, and date the counters by the last finish.
 if(run.running){bits.push(`running now, started ${formatIst(run.started_at)}`);if(run.last_success_at)bits.push(`last finished ${formatIst(run.last_success_at)}`);}
 else{
  if(run.finished_at)bits.push(formatIst(run.finished_at));
  else if(run.started_at)bits.push(`started ${formatIst(run.started_at)}`);
  if(run.age_seconds!=null)bits.push(agoText(run.age_seconds));
 }
 const counts:string[]=[];
 if(run.bars_written!=null)counts.push(`${run.bars_written} bar${run.bars_written===1?'':'s'} written`);
 if(run.symbols!=null)counts.push(`${run.symbols} symbols`);
 if(run.errors)counts.push(`${run.errors} error${run.errors===1?'':'s'}`);
 return {key:'refresh',label:'Last price refresh',value:[bits.join(' · '),counts.join(', ')].filter(Boolean).join(' — ')||'—',
  note:run.note||undefined,tone:run.errors?'stale':undefined};
}

function sessionRow(status:DataStatus):DataStatusRow{
 const session=status.session;
 if(!session?.state)return {key:'session',label:'Market',value:'Unknown',note:'The scanner did not report a session state.'};
 const label=SESSION_TEXT[session.state]||session.state;
 const extra=session.state==='open'&&session.close?`closes ${formatIst(session.close)}`
  :session.state==='pre_open'&&session.open?`opens ${formatIst(session.open)}`
  :session.next_open?`next open ${formatIst(session.next_open)}`:'';
 const reason=session.state==='holiday'?(session.reason==='weekend'?' (weekend)':' (exchange holiday)'):'';
 return {key:'session',label:'Market',value:`${label}${reason}${extra?` · ${extra}`:''}`,note:session.note||undefined};
}

// Everything the Data status popover renders, from one /api/state.data_status (plus the pill's own
// calendar age, so the historical case reads exactly like the pill).
export function dataStatusView(status:DataStatus|null|undefined,opts:{dataEnd?:string;ageDays?:number;staleLimit?:number;now?:Date;cache?:CacheProvenance|null;error?:string}={}):DataStatusView{
 const now=opts.now||new Date(),staleLimit=opts.staleLimit??DATA_STALE_DAYS;
 const tone=dataStatusTone(status,opts.ageDays,staleLimit,now);
 const conn=connectionView(opts.cache,{error:opts.error,now});
 // Nothing was ever cached: the panel says exactly that, instead of "Data source unknown".
 if(conn&&conn.mode==='offline')return {tone:conn.tone,live:false,kind:'No scan',headline:conn.text,detail:conn.detail,
  rows:[{key:'connection',label:'Connection',value:'Not answering',note:conn.detail,tone:conn.tone}],
  warning:{tone:conn.tone,text:conn.detail},expectation:'The page fills in as soon as the scanner answers. Nothing you have saved is affected.',
  a11y:conn.a11y};
 if(!status||status.error){
  const reason=status?.error?`Data provenance is unavailable: ${status.error}`:'This build of the scanner does not report where its prices come from.';
  return {tone,live:false,kind:'Unknown',headline:'Data source unknown',detail:reason,
   rows:[{key:'source',label:'Source',value:'Unknown',note:reason}],warning:null,
   expectation:'Ask the operator to restart the scanner if this does not clear.',
   a11y:`Data status: source unknown. ${reason}`};
 }
 const live=status.source?.live===true,delay=Number(status.provider?.delay_seconds)||0;
 const provider=status.provider?.label||status.provider?.id||'unknown provider';
 const bar=status.latest_bar?.end||status.latest_bar?.start||'';
 const lag=barLagSeconds(status,now);
 const stalled=status.stalled?.value===true,stale=status.stale?.value===true;
 const story=live?dataStory(status,now,opts.ageDays,staleLimit):null;
 const behind=!!story&&(!!story.pricesBehind);
 const kind=!live?'Historical':stalled?'Stalled':behind||stale?'Behind':story&&!story.open?'Closed':delay?`Live · ${Math.round(delay/60)} min delayed`:'Live';
 const headline=story
  ?(story.open?`${story.prices} · ${story.patterns}`:`Market closed · ${story.prices} · ${story.patterns}`)
  :`Historical · data to ${formatDataDate(bar||opts.dataEnd)||'an unknown date'}${opts.ageDays!=null?` (${opts.ageDays} day${opts.ageDays===1?'':'s'} old)`:''}`;
 const detail=story
  ?[story.next?`Patterns ${story.next}`:story.missing.includes('when patterns next update')?'The scanner does not report when patterns next update':'',
    `prices from ${provider}${lag!=null?`, ${agoText(lag+delay)}`:''}${delay?`, published ${Math.round(delay/60)} min late by design`:''}`]
    .filter(Boolean).map((t,i)=>i?t:t[0].toUpperCase()+t.slice(1)).join(' · ')
  :(status.source?.note||'Prices come from a stored research scan, not a live feed.');

 const p=status.patterns,frames=p?.by_timeframe||{},tfList=Object.keys(frames).length?Object.keys(frames):Object.keys(status.latest_bar?.by_timeframe||{});
 const rows:DataStatusRow[]=[
  {key:'source',label:'Source',value:live?`${status.source?.label||'Live store'} · ${provider}`:`${status.source?.label||'Stored scan'}${status.source?.store?` (${status.source.store})`:''}`,
   note:status.source?.note||undefined},
  {key:'bar',label:live?'Prices':'Newest bar',value:bar?`${formatIst(bar,now)} IST${lag!=null?` · ${agoText(lag+delay)}`:''}`:'Unknown',
   note:(live?undefined:status.latest_bar?.note)||undefined,tone:story?.pricesBehind?'stale':undefined},
 ];
 if(story){
  rows.push({key:'patterns',label:'Patterns',value:story.patterns.replace(/^Patterns:? ?/,'').replace(/^./,c=>c.toUpperCase())+(p?.latest&&!/\d{2}:\d{2}/.test(story.patterns)?` · ${formatIst(p.latest,now)} candle`:''),
   note:p?.error?`${p.note||''} (${p.error})`.trim():p?.note||undefined,tone:story.patternsBehind?'stale':undefined});
  rows.push({key:'patterns-next',label:'Next pattern update',
   value:story.next==='updating now'?'Updating now':p?.next_update?`${formatIst(p.next_update,now)} IST${istMs(p.next_update)!=null?` · ${inText(((istMs(p.next_update) as number)-now.getTime())/1000)}`:''}`:'Unknown',
   note:!p?'This scanner build does not report its pattern schedule; restarting the scanner adds it.'
    :'1-hour patterns update when each hourly candle completes (from 10:15), 4-hour at 13:15 and the close, daily and weekly after the close.'});
  if(tfList.length)rows.push({key:'timeframes',label:'By timeframe',
   value:tfList.map(tf=>{const v=frames[tf]?.scanned??status.latest_bar?.by_timeframe?.[tf];return `${tf} ${v?clockText(v,now):'not scanned'}${frames[tf]?.behind?' (behind)':''}`;}).join(' · ')});
 }
 rows.push(runRow(status));
 rows.push({key:'next',label:live?'Next price bar':'Next refresh',value:status.next_refresh?.at?`${formatIst(status.next_refresh.at,now)} IST${status.next_refresh.in_seconds!=null?` · ${inText(status.next_refresh.in_seconds)}`:''}`:'Not scheduled',
   note:status.next_refresh?.note||undefined});
 rows.push(sessionRow(status));
 if(delay)rows.push({key:'delay',label:'Vendor delay',value:`${Math.round(delay/60)} min, by design`,
  note:'A published delay is not staleness: the bar is correct, it simply arrives late.'});

 let warning:{tone:DataStatusTone;text:string}|null=null;
 if(story){
  const texts=[story.pricesBehind,story.patternsBehind].filter(Boolean) as string[];
  if(texts.length)warning={tone:stalled?'very-stale':'stale',text:texts.join(' ')};
  else if(status.last_run?.errors)warning={tone:'stale',text:`The last price refresh finished with ${status.last_run.errors} error${status.last_run.errors===1?'':'s'}; some symbols may be behind.`};
 }
 else if(stalled)warning={tone:'very-stale',text:`Ingestion looks stalled. ${status.stalled?.note||`No ingest cycle has finished in the last ${stallLimitMinutes(status)} minutes while the market is open.`}`};
 else if(stale)warning={tone:'stale',text:`Data is behind the market. ${status.stale?.note||'The newest bar is behind the newest completed session.'}`};
 else if((opts.ageDays??0)>staleLimit)warning={tone:tone==='very-stale'?'very-stale':'stale',text:`These prices are ${opts.ageDays} days old. Lists are stored scans, not current signals.`};

 const expectation=live
  ?(status.session?.state==='open'?'Prices update every 15 minutes. Patterns update only when a candle completes, so the pattern time trails the price time between candles; that is normal.'
   :'Prices and patterns update again once the market opens.')
  :'A stored scan only changes when the research data is refreshed.';
 const a11y=`Data status: ${headline}. ${detail}. ${warning?warning.text+' ':''}${expectation}`;
 // A scan IS on screen, the scanner simply is not answering. Everything below stays — those rows describe
 // the scan being shown and are still true of it — with the connection stated first and at the top.
 if(conn)return {tone:conn.tone,live,kind:'Last scan',headline:conn.text,detail:conn.detail,
  rows:[{key:'connection',label:'Connection',value:conn.mode==='lost'?'Not answering':'Reconnecting…',note:conn.detail,tone:conn.tone},
   {key:'taken',label:'Last scan',value:`${formatIst(opts.cache?.cached_at,now)} IST`,
    note:opts.cache?.cached_as_of?`The scan itself is as of ${formatIst(opts.cache.cached_as_of,now)}.`:undefined},
   ...rows],
  warning:{tone:conn.tone,text:conn.detail},
  expectation:'Nothing on this screen changes until the scanner answers again. Your account and saved plans are unaffected, and nothing can be simulated or sent live on a scan this old.',
  a11y:conn.a11y};
 return {tone:story?story.tone:tone,live,kind,headline,detail,rows,warning,expectation,a11y};
}
