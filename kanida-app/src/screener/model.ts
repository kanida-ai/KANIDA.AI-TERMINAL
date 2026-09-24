// Pure helpers for the screener UI: editing a draft definition, and the words each chip shows. No fetches, no
// React. The server stays the authority on what a definition MEANS — the "Reads as" sentence comes from
// /api/screener/describe, never from here.
import type {Condition,Definition,MetricOption,Side,Status,Strikes,Tick,Vocabulary,Window} from './types';

export const DEFAULT_WINDOW:Window={kind:'minutes',value:45};

export function newDefinition():Definition{
 return {universe:{kind:'all'},expiry:'nearest',strikes:{kind:'atm',below:5,above:5},liquid_only:false,
  conditions:[{metric:'oi',side:'CE',state:'up_cont',window:DEFAULT_WINDOW}]};
}

export function metricOf(v:Vocabulary|null,key:string):MetricOption|undefined{return v?.metrics.find(m=>m.key===key);}

/** A new condition, joined AND, copying the side and window of the one above so building a scanner reads
 *  like a sentence: "call OI ... AND call IV ..." */
export function addCondition(d:Definition,v:Vocabulary):Definition{
 const last=d.conditions[d.conditions.length-1];
 const metric=last?.metric==='oi'?'iv':'oi';
 const m=metricOf(v,metric);
 const side=m?.sides.length?(last?.side&&m.sides.includes(last.side)?last.side:'either'):null;
 const c:Condition={join:'and',metric,side,state:m?.default_state||'up',window:last?.window||DEFAULT_WINDOW};
 return {...d,conditions:[...d.conditions,c]};
}

/** Switching metric keeps whatever still makes sense (the side, the window) and resets what doesn't. */
export function withMetric(c:Condition,m:MetricOption):Condition{
 const side:Side|null=m.sides.length?(c.side&&m.sides.includes(c.side)?c.side:(m.sides.includes('either')?'either':m.sides[0])):null;
 const state=m.states.some(s=>s.key===c.state)?c.state:m.default_state;
 const window:Window=m.key==='maxpain'&&c.window.kind!=='open'?{kind:'open'}:c.window;
 return {...c,metric:m.key,side,state,window};
}

export function updateCondition(d:Definition,i:number,patch:Partial<Condition>):Definition{
 return {...d,conditions:d.conditions.map((c,j)=>j===i?{...c,...patch}:c)};
}
export function removeCondition(d:Definition,i:number):Definition{
 const next=d.conditions.filter((_c,j)=>j!==i);
 if(next[0])next[0]={...next[0],join:undefined};
 return {...d,conditions:next};
}
export function toggleJoin(d:Definition,i:number):Definition{
 return updateCondition(d,i,{join:d.conditions[i].join==='or'?'and':'or'});
}

export function windowLabel(w:Window){
 if(w.kind==='custom')return `${w.from}–${w.to}`;
 if(w.kind==='open')return 'Since open';
 return w.kind==='minutes'?`${w.value} min`:`${w.value} readings`;
}
export const sameWindow=(a:Window,b:Window)=>JSON.stringify(a)===JSON.stringify(b);

export function strikesLabel(s:Strikes,v:Vocabulary|null){
 if(s.kind==='atm')return s.below===s.above?(s.below===0?'ATM only':`ATM ±${s.below}`):`ATM −${s.below} / +${s.above}`;
 if(s.kind!=='delta')return `${s.kind.toUpperCase()}, ${s.depth} strikes`;
 const key=s.band;
 const band=v?.delta_bands.find(b=>b.key===key);
 return band?`${band.label} (|Δ| ${band.lo.toFixed(2)}–${band.hi.toFixed(2)})`:'Delta band';
}
export function universeLabel(d:Definition,v:Vocabulary|null){
 if(d.universe.kind==='symbols')return d.universe.symbols.length<=2?d.universe.symbols.join(', '):`${d.universe.symbols.length} symbols`;
 return v?.universes.find(u=>u.key===d.universe.kind)?.label||'Indices + F&O stocks';
}

export const STATUS_LABEL:Record<Status,string>={new:'NEW MATCH',still:'STILL MATCHING',strengthening:'STRENGTHENING',
 weakening:'WEAKENING',ended:'CONDITION ENDED'};
/** Tone repeats the word, never adds a verdict: green = a match running, amber = losing pace, grey = over. */
export const STATUS_TONE:Record<Status,'green'|'amber'|'neutral'>={new:'green',still:'green',strengthening:'green',
 weakening:'amber',ended:'neutral'};
export const TICK_GLYPH:Record<Tick,string>={none:'·',unseen:' ',gap:'⋯',new:'●',still:'■',strengthening:'▲',weakening:'▼',ended:'○'};

/** "Mon 21 Sep, 11:15" → the time only. */
export const hhmm=(at?:string|null)=>String(at||'').slice(11,16);
/** The next 15-min reading after `as_of`, said as a time — never a promise, a clock sum. */
export function nextReading(asOf?:string|null){
 const t=hhmm(asOf);if(!t)return '';
 const [h,m]=t.split(':').map(Number);const total=h*60+m+15;
 if(total>15*60+30)return '';
 return `${String(Math.floor(total/60)).padStart(2,'0')}:${String(total%60).padStart(2,'0')}`;
}
export function expiryShort(e?:string){
 if(!e)return '';
 const M=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
 const [,mm,dd]=e.split('-');return `${Number(dd)} ${M[Number(mm)-1]||''}`;
}
/** Reading times a custom window can start or end on: the session's 15-min marks, 09:30–15:30. */
export const READING_TIMES=(()=>{const out:string[]=[];for(let m=9*60+30;m<=15*60+30;m+=15)out.push(`${String(Math.floor(m/60)).padStart(2,'0')}:${String(m%60).padStart(2,'0')}`);return out;})();
