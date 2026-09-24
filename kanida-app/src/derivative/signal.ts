// BLOCK 1, MIDDLE PANE — the 15-minute market state, as a structured object.
//
// THE STATE IS COMPUTED, THEN EXPLAINED — never the other way round. Everything the pane draws, and every
// sentence it prints, comes off one object per reading. That object is the deliverable: the same fields
// have to serve the screen, the narrative, an API and eventually a system that trades on them, so no
// figure is invented at render time and no sentence is assembled from anything the object does not hold.
//
// TWO WINDOWS, AND EACH SAYS WHICH IT IS.
//   · THE INTERVAL — this reading against the one immediately before it. Every Δ on screen is this.
//   · THE EPISODE  — the tab's hour-wide rule, which is what "persistent for 60 minutes" is counted on.
// Mixing them is how a panel comes to claim an hour of persistence from a fifteen-minute move.
//
// WHAT IT MAY CLAIM. A behaviour — call writing, put buying — is an INTERPRETATION of open interest and
// premium read together, and it is never printed without the evidence that contradicts or fails to confirm
// it standing beside it. `conflicting` is not an optional extra: where the underlying's own move could
// account for the premium, or volatility is not confirming, or the other side is silent, it is said.
import {DASH,MINUS,RUPEE,SIGNAL_RULE_VERSION,clock,compact,price as priceText,strike as strikeText} from './logic';
import {DIRECTIONAL,PACE_DOWN,PACE_UP,observe,type ReadingObservation,type Rung,type SideReading} from './summary';
import type {OiGrid} from './types';

/** The closed state vocabulary. Nothing outside this reaches a reader. */
export type State='opening'|'appeared'|'building'|'held'|'strengthened'|'broadened'|'concentrated'|'shifted'
 |'slowed'|'fading'|'reversing'|'balanced'|'mixed'|'not_observed'|'no_baseline';
export const STATE_LABEL:Record<State,string>={
 opening:'Opening read',appeared:'Appeared',building:'Building',held:'Held',strengthened:'Strengthened',broadened:'Broadened',
 concentrated:'Concentrated',shifted:'Shifted',slowed:'Slowed',fading:'Fading',reversing:'Reversing',
 balanced:'Balanced',mixed:'Mixed',not_observed:'Not observed',no_baseline:'No baseline yet',
};
/** What each state MEANS for colour. Green is activity expanding, red is contracting, amber is losing
 *  strength or carrying a caveat, grey is unchanged. None of them is a market direction. */
export const STATE_TONE:Record<State,'up'|'down'|'warn'|'flat'>={
 opening:'flat',appeared:'up',building:'up',held:'up',strengthened:'up',broadened:'up',
 concentrated:'warn',shifted:'warn',slowed:'warn',fading:'warn',reversing:'down',
 balanced:'flat',mixed:'flat',not_observed:'warn',no_baseline:'flat',
};

export type Breadth='isolated'|'clustered'|'dispersed'|'none';
export type KeyStrike={strike:number;side:'CE'|'PE';note:string;lead:boolean};
export type Evidence={text:string};

/** ONE 15-MINUTE INTELLIGENCE OBJECT. The screen, the narrative, an API and anything that reads this
 *  downstream all take the same fields. */
export type SignalState={
 timestamp:string|null;previous_timestamp:string|null;interval_minutes:number|null;
 current_timestamp:string|null;market_state:State;
 /** What each side did this interval: building / reducing / quiet / opening / not_measured. */
 call_state:string;put_state:string;strike_cluster:number[];

 state:State;state_change:string;

 side:'CE'|'PE'|null;row:'calls'|'puts'|null;
 location:string;strike_range:number[];leading_strike:number|null;

 /** Persistence is only reported when the hour-wide window is full. In the first hour of a session the
  *  state comes off the fifteen-minute interval and these stay null rather than borrowing a number the
  *  window cannot support. */
 persistence_since:string|null;persistence_minutes:number|null;consecutive_readings:number;
 persistence_known:boolean;persistence_evidence_available:boolean;persistence_status:'established'|'none';

 breadth_previous:number|null;breadth_current:number;breadth_direction:'wider'|'narrower'|'same'|'none';
 breadth:Breadth;

 /** The LEADING CONTRACT's own premium — a real instrument. A side's five strikes summed together are not
  *  one, so their total is never presented as a price. */
 price_change:number|null;price_change_pct:number|null;price_level:number|null;price_contract:string;

 call_oi_change:number|null;put_oi_change:number|null;
 call_oi_level:number|null;put_oi_level:number|null;
 call_iv_change:number|null;put_iv_change:number|null;iv_change:number|null;iv_level:number|null;
 iv_reason:string;

 pcr_previous:number|null;pcr_current:number|null;pcr_change:number|null;
 max_pain_previous:number|null;max_pain_current:number|null;max_pain_change:number|null;

 supporting_evidence:Evidence[];conflicting_evidence:Evidence[];
 opposing_side_state:string;

 /** The regime the evidence is consistent with, or null where it is not resolvable. Never printed without
  *  `conflicting_evidence` beside it. */
 regime:string|null;
 tags:string[];
 key_strikes:KeyStrike[];

 plain_language_read:string;plain_language_detail:string;
 plain_language_headline:string;plain_language_evidence:string;
 /** The side's behaviour word from the tab's table (writing/buying/…), and the size of its OI change this
  *  interval — what the session chain compares reading to reading. */
 behaviour:string;weight:number;
 /** Which engine produced this object. Stored with every snapshot, so a later engine never passes for this one. */
 engine_version:string;rules_version:string;
 covered:boolean;
};

/** ABSENCE IS NOT ZERO. Number(null) is 0 and finite, so a bare coercion turns a field that was never
 *  captured into a captured nought — the one mistake this tab exists to avoid. */
const num=(v:unknown)=>{
 if(v==null||v==='')return null;
 const n=typeof v==='number'?v:Number(v);
 return Number.isFinite(n)?n:null;
};
const key=(at:unknown)=>String(at||'').slice(0,16);
type Point={at?:string|null;[k:string]:unknown};
const byReading=(rows:Point[]|null|undefined)=>{
 const out=new Map<string,Point>();
 for(const row of rows||[])if(row&&row.at)out.set(key(row.at),row);
 return out;
};
const round=(v:number|null,places=2)=>v==null?null:Math.round(v*10**places)/10**places;

export const signedPct=(v:number|null)=>v==null?DASH
 :`${v>0?'+':v<0?MINUS:''}${Math.abs(v).toFixed(0)}%`;
export const signedLots=(v:number|null)=>v==null?DASH:`${v>0?'+':v<0?MINUS:''}${compact(Math.abs(v))}`;
export const signedMoney=(v:number|null)=>v==null?DASH
 :`${v>0?'+':v<0?MINUS:''}${RUPEE}${Math.abs(v).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2})}`;
export const signedNum=(v:number|null,places=1)=>v==null?DASH
 :`${v>0?'+':v<0?MINUS:''}${Math.abs(v).toFixed(places)}`;
export const strikeOf=(v:number|null)=>v==null?DASH:strikeText(v);
export {priceText};

/** The rung that moved most on one side at one reading — a real contract, which is why the price figure
 *  reads off it rather than off the sum of five. */
function leadRung(ladder:Rung[],row:'calls'|'puts'):Rung|null{
 let best:Rung|null=null;
 for(const rung of ladder){
  if(rung.row!==row||rung.status==='not_captured')continue;
  if(!best||Math.abs(num(rung.oi_added)||0)>Math.abs(num(best.oi_added)||0))best=rung;
 }
 return best&&Math.abs(num(best.oi_added)||0)>0?best:null;
}

/** The tab's own behaviour table, in the reader's words. Only ever shown with its conflicts. */
const REGIME:Record<string,string>={
 buying:'Buying',writing:'Writing',short_covering:'Short covering',buyers_exiting:'Buyers exiting'};
const regimeOf=(row:'calls'|'puts'|null,behaviour:string):string|null=>{
 const word=REGIME[behaviour];
 if(!word||!row)return null;
 return `${row==='puts'?'Put':'Call'} ${word.toLowerCase()}`;
};

/** THE FIFTEEN-MINUTE SUBJECT, for readings the hour-wide window cannot yet reach.
 *
 *  It answers only what the interval can answer: which side moved, what that movement was, which strikes
 *  carried it, and which led. It reports NO elapsed time — the interval reading has none to give, and a
 *  state word here describes this fifteen minutes and nothing before it. */
type Fallback={row:'calls'|'puts';side:'CE'|'PE';behaviour:string;state:State;
 strikes:number[];lead:number|null;both:boolean};
function fallbackSide(cI:SideReading|null,pI:SideReading|null,ladder:Rung[],_rungs:number[]):Fallback|null{
 const pick=(a:SideReading|null,b:SideReading|null)=>{
  const aw=a&&DIRECTIONAL.indexOf(a.behaviour)>=0?a.weight:0;
  const bw=b&&DIRECTIONAL.indexOf(b.behaviour)>=0?b.weight:0;
  if(aw<=0&&bw<=0)return null;
  return bw>aw?b:a;
 };
 const won=pick(cI,pI);
 if(!won)return null;
 // THE SAME BALANCE TEST THE EPISODE USES. Two sides within a quarter of each other are not one side
 // acting — they are both acting, and naming the marginally larger one is an over-claim. Without this the
 // same book reads "Call writing" in the first hour and "Both sides active" after it.
 const cw=cI&&DIRECTIONAL.indexOf(cI.behaviour)>=0?cI.weight:0;
 const pw=pI&&DIRECTIONAL.indexOf(pI.behaviour)>=0?pI.weight:0;
 const both=cw>0&&pw>0&&Math.max(cw,pw)<=Math.min(cw,pw)*1.25
  &&(EXIT_BEHAVIOURS.indexOf(cI!.behaviour)>=0)!==(EXIT_BEHAVIOURS.indexOf(pI!.behaviour)>=0);
 const row=won.row,side=won.side;
 // The strikes come off the same ladder the key-strike chips do, so the sentence and the chips cannot
 // disagree about where the reading happened.
 const moved=ladder.filter(r=>r.row===row&&(r.status==='added'||r.status==='reduced')
  &&Math.abs(num(r.oi_added)||0)>0);
 if(!moved.length)return null;
 const strikes=moved.map(r=>r.strike).sort((a,b)=>a-b);
 const lead=moved.reduce((best,r)=>
  Math.abs(num(r.oi_added)||0)>Math.abs(num(best.oi_added)||0)?r:best,moved[0]).strike;
 // EXITS ARE NOT BUILDS. Positions being closed read as reversing, whichever side they sit on.
 const state:State=both?'mixed'
  :EXIT_BEHAVIOURS.indexOf(won.behaviour)>=0?'reversing':'building';
 return {row,side,behaviour:won.behaviour,state,strikes,lead,both};
}
const EXIT_BEHAVIOURS=['short_covering','buyers_exiting'];

/** The episode state, mapped into the pane's vocabulary. */
function stateOf(side:SideReading|null,both:boolean):State{
 if(!side)return 'no_baseline';
 if(side.state==='no_baseline')return 'no_baseline';
 if(side.state==='not_observed')return 'not_observed';
 if(both)return 'mixed';
 switch(side.state){
  case 'appeared':return 'appeared';
  case 'continued':return side.scans<=2?'building':'held';
  case 'strengthened':return 'strengthened';
  case 'broadened':return 'broadened';
  case 'narrowed':return 'concentrated';
  case 'shifted':return 'shifted';
  case 'slowed':return 'slowed';
  case 'faded':return 'fading';
  case 'unwound':return 'reversing';
  default:return 'balanced';
 }
}

const breadthOf=(n:number,contiguous:boolean):Breadth=>
 n<=0?'none':n===1?'isolated':contiguous?'clustered':'dispersed';

function minutesText(m:number|null){
 if(m==null||m<=0)return '';
 if(m<60)return `${m} min`;
 const h=Math.floor(m/60),rest=m%60;
 return `${h} hr${h===1?'':'s'}${rest?` ${rest} min`:''}`;
}
/** How persistence is SAID — and whether it is said at all. It appears only once the same behaviour has held
 *  across comparable readings, as the time it has held since; before that there is nothing to say, so nothing is. */
export function persistenceText(s:SignalState){
 if(!s.persistence_evidence_available||!s.persistence_since)return '';
 return `Since ${clock(s.persistence_since)}`;
}

export type SignalInput={grid?:OiGrid|null;pcr?:{points?:Point[]}|null;
 maxPain?:{points?:Point[]}|null;iv?:{points?:Point[]}|null};

/** EVERY READING THE GRID HOLDS, newest first, as one state object each. */
/** The engine that wrote an object. Bump it whenever a rule or a word this file produces changes. */
export const ENGINE_VERSION='pane/3.3 2026-09-21';

/** `intervalOnly`: describe every reading from its own fifteen minutes alone — the SNAPSHOT mode. Session context
 *  (persistence, held/broadened/shifted…) is then added by `chainStep` from STORED earlier snapshots, never by
 *  re-walking earlier readings on this reading's contracts. */
export function states(input:SignalInput,opts:{intervalOnly?:boolean}={}):SignalState[]{
 const grid=input.grid;
 const walk=observe(grid);
 if(!walk.length)return [];
 const pcrMap=byReading(input.pcr?.points),mpMap=byReading(input.maxPain?.points),
  ivMap=byReading(input.iv?.points);
 const rungs=Array.from(new Set((grid?.rows||[]).map(r=>num(r.strike))
  .filter((v):v is number=>v!=null))).sort((a,b)=>a-b);

 // WHERE THE MONEY IS relative to the at-the-money strike the grid was built on. Traders place activity against
 // ATM before they place it against a number; the strikes themselves are in the evidence sentence and the chips.
 const atm=num((grid as any)?.atm_strike);
 const rel=(list:number[])=>{
  if(atm==null||!list.length)return '';
  const lo=Math.min(...list),hi=Math.max(...list);
  if(lo===hi&&lo===atm)return 'at ATM';
  if(lo>=atm&&hi>atm)return 'above ATM';
  if(hi<=atm&&lo<atm)return 'below ATM';
  return 'around ATM';
 };
 const span=(list:number[],code:string)=>{
  const s=list.slice().sort((a,b)=>a-b);
  return !s.length?'':s.length===1?`${strikeText(s[0])} ${code}`:`${strikeText(s[0])}–${strikeText(s[s.length-1])} ${code}`;
 };

 const built=walk.map((obs:ReadingObservation,i)=>{
  const here=key(obs.at),before=key(obs.previous_at);
  const prev=i>0?walk[i-1]:null;
  const cI=obs.calls_interval,pI=obs.puts_interval;
  const ladderRows=obs.ladder||[];
  const leadCall=leadRung(ladderRows,'calls'),leadPut=leadRung(ladderRows,'puts');
  const first=!obs.previous_at;

  // --- WHICH SIDE THE READING IS ABOUT, on the episode the persistence is counted on. -------------------------
  const cW=obs.calls.weight,pW=obs.puts.weight;
  const row:'calls'|'puts'|null=cW===0&&pW===0?null:(pW>cW?'puts':'calls');
  const side=row==='puts'?obs.puts:row==='calls'?obs.calls:null;
  const other=row==='puts'?obs.calls:obs.puts;
  // MIXED MEANS THE SIDES CONTRADICT — one opening positions while the other closes them, at comparable size.
  // Both sides adding is two behaviours, not a conflict (found live 21 Sep 09:45: calls written, puts bought,
  // labelled MIXED). And a lack of history is never a market state at all.
  const isExit=(b:string)=>EXIT_BEHAVIOURS.indexOf(b)>=0;
  const conflict=cW>0&&pW>0&&Math.max(cW,pW)<=Math.min(cW,pW)*1.25
   &&isExit(obs.calls.behaviour)!==isExit(obs.puts.behaviour);
  const basis=side||(obs.calls.state!=='quiet'?obs.calls:obs.puts);
  const episodeState=stateOf(basis,conflict&&!!side&&side.state!=='not_observed'&&side.state!=='no_baseline');

  // --- THE FIRST HOUR. The hour-wide window is not full, the fifteen minutes are: the interval speaks. -------
  const iv=first?null:fallbackSide(cI,pI,ladderRows,rungs);
  const useInterval=(opts.intervalOnly||episodeState==='no_baseline')&&!first;
  const intervalMeasured=!!((cI&&cI.total)||(pI&&pI.total));
  const state:State=first?'opening'
   :useInterval?(iv?iv.state:(intervalMeasured?'balanced':'not_observed'))
   :episodeState;
  // Persistence is an OUTPUT of accumulated evidence: it exists once the same behaviour has held across more
  // than one comparable reading, and not before. Until then it is simply absent — never explained.
  const persistenceKnown=!useInterval&&!first&&!!side&&side.scans>=2&&!!side.first_at;

  const row2:'calls'|'puts'|null=useInterval?(iv?iv.row:null):row;
  const sideCode:'CE'|'PE'|null=useInterval?(iv?iv.side:null):(side?side.side:null);
  const behaviour=useInterval?(iv?iv.behaviour:'none'):(side?side.behaviour:'none');
  const strikeRange=useInterval?(iv?iv.strikes.slice():[]):(side?side.strikes.slice():[]);
  const lead=useInterval?(iv?iv.lead:null):(side?side.lead:null);
  const contiguous=strikeRange.length>1
   &&rungs.indexOf(strikeRange[strikeRange.length-1])-rungs.indexOf(strikeRange[0])===strikeRange.length-1;

  // BREADTH IS COMPARED LIKE WITH LIKE, OR NOT AT ALL. The first comparable reading has no earlier set to be
  // broader than — "5 strikes, up from 0" at 09:45 was a change invented against nothing.
  const prevIv=prev&&prev.previous_at?fallbackSide(prev.calls_interval,prev.puts_interval,prev.ladder||[],rungs):null;
  const prevSide=prev?(row2==='puts'?prev.puts:prev.calls):null;
  const prevEpisodeReal=!!prevSide&&prevSide.state!=='no_baseline'&&prevSide.state!=='not_observed';
  const breadthPrev:number|null=useInterval
   ?(prevIv&&prevIv.row===row2?prevIv.strikes.length:null)
   :(prevEpisodeReal?prevSide!.strikes.length:null);
  const breadthNow=strikeRange.length;
  const breadthDir:SignalState['breadth_direction']=breadthPrev==null||(!breadthNow&&!breadthPrev)?'none'
   :breadthNow>breadthPrev?'wider':breadthNow<breadthPrev?'narrower':'same';

  // --- the metrics. Every Δ is over the INTERVAL, never the episode. -----------------------------------------
  const leadForPrice=row2==='puts'?(leadPut||leadCall):(leadCall||leadPut);
  const priceChange=leadForPrice?num(leadForPrice.price_change):null;
  const priceLevel=leadForPrice?num(leadForPrice.price_level):null;
  const priceBefore=priceLevel!=null&&priceChange!=null?priceLevel-priceChange:null;
  const pricePct=priceBefore&&priceBefore!==0&&priceChange!=null
   ?round(priceChange/Math.abs(priceBefore)*100,0):null;

  const callOi=cI&&cI.total?num(cI.total.change):null;
  const putOi=pI&&pI.total?num(pI.total.change):null;
  const levelOf=(r:'calls'|'puts')=>ladderRows.filter(x=>x.row===r)
   .reduce<number|null>((sum,x)=>{const v=num(x.oi_level);return v==null?sum:(sum||0)+v;},null);

  const ivNow=ivMap.get(here),ivBefore=ivMap.get(before);
  const ivLevel=ivNow?num(ivNow.iv_pct):null,ivPrev=ivBefore?num(ivBefore.iv_pct):null;
  const ivChange=ivLevel!=null&&ivPrev!=null?round(ivLevel-ivPrev,1):null;
  const ceIv=ivNow?num(ivNow.ce_iv_pct):null,peIv=ivNow?num(ivNow.pe_iv_pct):null;
  const ceIvPrev=ivBefore?num(ivBefore.ce_iv_pct):null,peIvPrev=ivBefore?num(ivBefore.pe_iv_pct):null;

  const pcrNow=pcrMap.get(here),pcrBefore=pcrMap.get(before);
  const pcrCur=pcrNow?num(pcrNow.pcr_oi):null,pcrPrev=pcrBefore?num(pcrBefore.pcr_oi):null;
  const mpNow=mpMap.get(here),mpBefore=mpMap.get(before);
  const mpCur=mpNow?num(mpNow.max_pain_strike):null,mpPrev=mpBefore?num(mpBefore.max_pain_strike):null;

  // --- PER SIDE: what each side's contracts did this interval, where, and with what premium. ----------------
  const movedOn=(r:'calls'|'puts')=>ladderRows.filter(x=>x.row===r&&(x.status==='added'||x.status==='reduced')
   &&Math.abs(num(x.oi_added)||0)>0);
  const sideWord=(r:'calls'|'puts',total:number|null,beh:string|undefined)=>{
   if(total==null)return 'not_measured';
   if(!movedOn(r).length)return 'quiet';
   if(beh&&isExit(beh))return 'reducing';
   return total>0?'building':total<0?'reducing':'quiet';
  };
  const callState=first?'opening':sideWord('calls',callOi,cI?.behaviour);
  const putState=first?'opening':sideWord('puts',putOi,pI?.behaviour);
  const pxWord=(r:'calls'|'puts')=>{
   const moved=movedOn(r);
   const px=moved.reduce((s,x)=>s+(num(x.price_change)||0),0);
   const ref=moved.reduce((s,x)=>s+Math.abs(num(x.price_level)||0),0);
   if(!moved.length)return '';
   if(ref>0&&Math.abs(px)<0.01*ref)return 'with prices little changed';
   return px<0?'while prices declined':px>0?'while prices rose':'with prices unchanged';
  };
  const oiClause=(r:'calls'|'puts',total:number|null,leadIt:boolean)=>{
   const moved=movedOn(r);
   if(total==null||!moved.length||total===0)return '';
   const code=r==='puts'?'PE':'CE';
   const who=r==='puts'?'Put':'Call';
   const top=moved.slice().sort((a,b)=>Math.abs(num(b.oi_added)||0)-Math.abs(num(a.oi_added)||0))[0];
   return `${who} OI ${total>0?'rose':'fell'} ${compact(Math.abs(total))} across ${span(moved.map(x=>x.strike),code)} `
    +`${pxWord(r)}`.trim()+(leadIt&&top?`; ${strikeText(top.strike)} ${code} leads`:'');
  };

  // --- WHAT SUPPORTS THE READ, AND WHAT DOES NOT. Market evidence only — never a note about the software. ----
  const supporting:Evidence[]=[];const conflicting:Evidence[]=[];
  const sideOi=row2==='puts'?putOi:callOi;
  const otherOi=row2==='puts'?callOi:putOi;
  const otherRow:'calls'|'puts'=row2==='puts'?'calls':'puts';
  const directional=['appeared','building','held','strengthened','broadened','concentrated','shifted',
   'slowed','reversing','mixed'].indexOf(state)>=0;
  if(row2&&directional){
   const mine=oiClause(row2,sideOi,true);
   if(mine)supporting.push({text:mine+'.'});
   const material=otherOi!=null&&sideOi!=null&&Math.abs(otherOi)>=Math.abs(sideOi)*0.25;
   if(material){const theirs=oiClause(otherRow,otherOi,false);if(theirs)supporting.push({text:theirs+'.'});}
   else if(sideOi!=null)supporting.push({text:`${otherRow==='puts'?'Put':'Call'} activity changed only marginally.`});
   if(ivChange!=null&&Math.abs(ivChange)<0.2)conflicting.push({text:'IV was unchanged.'});
   if(breadthDir==='narrower')conflicting.push({text:`Activity narrowed to ${breadthNow} strike${breadthNow===1?'':'s'} from ${breadthPrev}.`});
  }

  // THE REGIME WORD — writing, buying, covering — only when this side clearly carried the interval. When the
  // other side moved on a comparable scale the headline says "activity" and the evidence names both.
  const dominant=sideOi!=null&&(otherOi==null||Math.abs(sideOi)>Math.abs(otherOi)*1.25);
  // …and only when the premium on that side moved enough to say so. Found live 21 Sep 10:00: "Put writing"
  // headlined over "Put OI rose … with prices little changed" — the word outran its own evidence.
  const pxClear=!!row2&&/declined|rose/.test(pxWord(row2));
  const regime=state==='mixed'||!row2||!dominant||!pxClear?null:regimeOf(row2,behaviour);
  const opposing=otherRow==='puts'?putState:callState;

  // --- THE TAGS. Three at most, each about the market. ---------------------------------------------------
  const tags:string[]=[];
  if(directional){
   if(breadthDir==='wider')tags.push('Broader activity');
   else if(breadthDir==='narrower')tags.push('Narrowing');
   if(sideOi!=null&&otherOi!=null&&Math.abs(sideOi)>Math.abs(otherOi)*1.5)
    tags.push(`${row2==='puts'?'Puts':'Calls'} stronger`);
   else if(sideOi!=null&&otherOi!=null&&Math.abs(otherOi)>Math.abs(sideOi))
    tags.push(`${otherRow==='puts'?'Puts':'Calls'} larger`);
   if(ivChange!=null&&Math.abs(ivChange)<0.2)tags.push('IV not confirming');
  }

  // --- KEY STRIKES: only the ones this reading's behaviour is about. -----------------------------------
  const keyStrikes:KeyStrike[]=!row2||!directional?[]:movedOn(row2)
   .sort((a,b)=>Math.abs(num(b.oi_added)||0)-Math.abs(num(a.oi_added)||0))
   .slice(0,4)
   .map((r,n)=>({strike:r.strike,side:r.side,lead:n===0,
    note:n===0?'Highest activity':r.joined?'New activity':r.status==='reduced'?'Reducing':'Building'}));

  // --- THE OPENING READ: where open interest is concentrated near the money, as levels. ------------------
  const heavy=(r:'calls'|'puts')=>ladderRows.filter(x=>x.row===r&&num(x.oi_level)!=null&&(num(x.oi_level) as number)>0)
   .sort((a,b)=>(num(b.oi_level) as number)-(num(a.oi_level) as number)).slice(0,3).map(x=>x.strike);
  const openCalls=first?heavy('calls'):[],openPuts=first?heavy('puts'):[];

  // --- THE WORDS. What changed, where — then the one sentence of evidence. ------------------------------
  const who=row2==='puts'?'Put':'Call';
  const subject=regime||`${who} activity`;
  const where=rel(strikeRange);
  const prevLead=prevSide&&prevEpisodeReal?prevSide.lead:null;
  const joined=side?side.joined||[]:[];
  const prevSet=prevSide&&prevEpisodeReal?prevSide.strikes:[];
  const into=joined.length&&prevSet.length
   ?(Math.min(...joined)>Math.max(...prevSet)?'higher':Math.max(...joined)<Math.min(...prevSet)?'lower':'more')
   :'more';
  let read='';
  switch(state){
   case 'opening':{
    const parts:string[]=[];
    if(openCalls.length)parts.push(`Calls concentrated around ${span(openCalls,'CE')}`);
    if(openPuts.length)parts.push(`${openCalls.length?'puts':'Puts'} around ${span(openPuts,'PE')}`);
    read=parts.join('; ')||'Opening read';break;}
   case 'building':read=`${subject} building ${where}`;break;
   case 'appeared':read=`${subject} appears ${where}`;break;
   case 'held':read=`${subject} holding ${where}`;break;
   case 'strengthened':read=`${subject} strengthens ${where}`;break;
   case 'broadened':read=`${subject} broadens into ${into} strikes`;break;
   case 'concentrated':read=`${subject} narrows to ${span(strikeRange,sideCode||'')}`;break;
   case 'shifted':read=prevLead!=null&&lead!=null
    ?`Leading ${who.toLowerCase()} activity shifts from ${strikeText(prevLead)} to ${strikeText(lead)} ${sideCode||''}`
    :`Leading ${who.toLowerCase()} activity shifts strike`;break;
   case 'slowed':read=`${subject} losing momentum ${where}`;break;
   case 'fading':read=`Earlier ${(regime||`${who} activity`).toLowerCase()} is fading`;break;
   case 'reversing':read=`${who} positions being reduced ${where}`;break;
   case 'mixed':read=`Call positions ${callState==='reducing'?'reducing':'building'} while put positions `
    +`${putState==='reducing'?'reduce':'build'}`;break;
   case 'balanced':read='No material change in market structure';break;
   case 'not_observed':read='No comparable values captured at this reading';break;
   default:read='';
  }
  read=read.replace(/\s+/g,' ').trim();
  const evidence=supporting.map(e=>e.text.replace(/\.$/,'')).join('. ')+(supporting.length?'.':'');

  const changeWord=STATE_LABEL[state];
  const known=persistenceKnown&&!!side;
  return {
   timestamp:obs.at,previous_timestamp:obs.previous_at,
   current_timestamp:obs.at,
   interval_minutes:obs.previous_at&&obs.at?15:null,
   state,state_change:changeWord,market_state:state,
   call_state:callState,put_state:putState,
   side:sideCode,row:row2,
   location:where,strike_range:first?openCalls:strikeRange,strike_cluster:first?openCalls:strikeRange,
   leading_strike:lead,
   persistence_since:known?side!.first_at:null,
   persistence_minutes:known?side!.elapsed_minutes:null,
   consecutive_readings:known?side!.scans:0,
   persistence_known:known,persistence_evidence_available:known,
   persistence_status:known?'established':'none',
   breadth_previous:breadthPrev,breadth_current:breadthNow,breadth_direction:breadthDir,
   breadth:breadthOf(breadthNow,contiguous),
   price_change:round(priceChange),price_change_pct:pricePct,price_level:round(priceLevel),
   price_contract:leadForPrice?`${strikeText(leadForPrice.strike)} ${leadForPrice.side}`:'',
   call_oi_change:callOi,put_oi_change:putOi,
   call_oi_level:levelOf('calls'),put_oi_level:levelOf('puts'),
   call_iv_change:ceIv!=null&&ceIvPrev!=null?round(ceIv-ceIvPrev,1):null,
   put_iv_change:peIv!=null&&peIvPrev!=null?round(peIv-peIvPrev,1):null,
   iv_change:ivChange,iv_level:ivLevel,
   iv_reason:String(ivNow?.reason_text||''),
   pcr_previous:round(pcrPrev),pcr_current:round(pcrCur),
   pcr_change:pcrCur!=null&&pcrPrev!=null?round(pcrCur-pcrPrev):null,
   max_pain_previous:mpPrev,max_pain_current:mpCur,
   max_pain_change:mpCur!=null&&mpPrev!=null?mpCur-mpPrev:null,
   supporting_evidence:supporting,conflicting_evidence:conflicting,
   opposing_side_state:opposing,
   regime,tags:tags.slice(0,3),key_strikes:keyStrikes,
   plain_language_read:read,plain_language_detail:evidence,
   plain_language_headline:read,plain_language_evidence:evidence,
   behaviour,weight:Math.abs(sideOi||0),engine_version:ENGINE_VERSION,rules_version:SIGNAL_RULE_VERSION,
   covered:obs.covered,
  } as SignalState;
 });
 return built.reverse();
}

// ================================================================================================================
// THE SESSION CHAIN — persistence and state transitions from STORED snapshots.
//
// A reading is described from its own fifteen minutes on its own at-the-money contracts (`states(…,{intervalOnly})`)
// and stored once. Everything that needs the past — held, broadened, shifted, persistence — is decided HERE, by
// comparing this reading with the PREVIOUS STORED SNAPSHOT: never by re-reading earlier readings on this reading's
// contracts. That is the whole fix for the 21 Sep repaint.
//
// Strikes are compared as STRIKES, not as ATM-relative slots. When the at-the-money strike moves by one, the two
// readings share most of their contracts; "activity held at 23,400 CE" must survive that, and a slot that merely
// renamed itself must not look like new activity.
// ================================================================================================================
const RUN_STATES:State[]=['building','appeared','held','strengthened','broadened','concentrated','shifted','slowed','reversing'];
const minutesBetween=(a:string|null,b:string|null)=>{
 const t=(s:string|null)=>{const m=String(s||'').match(/(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/);
  return m?Date.UTC(+m[1],+m[2]-1,+m[3],+m[4],+m[5])/60000:null;};
 const x=t(a),y=t(b);return x==null||y==null?null:y-x;
};

export function chainStep(prev:SignalState|null,cur:SignalState):SignalState{
 const none={persistence_since:null,persistence_minutes:null,consecutive_readings:0,persistence_known:false,
  persistence_evidence_available:false,persistence_status:'none' as const};
 const out:SignalState={...cur,...none};
 if(!prev||cur.state==='opening'||cur.state==='not_observed'||cur.state==='mixed')return out;
 const inRun=(s:SignalState)=>RUN_STATES.indexOf(s.state)>=0;
 const closing=(s:SignalState)=>s.state==='reversing'||EXIT_BEHAVIOURS.indexOf(s.behaviour)>=0;
 const who=(s:SignalState)=>s.row==='puts'?'Put':'Call';
 const subjectOf=(s:SignalState)=>s.regime||`${who(s)} activity`;
 const say=(state:State,read:string)=>{out.state=state;out.state_change=STATE_LABEL[state];out.market_state=state;
  out.plain_language_read=read.replace(/\s+/g,' ').trim();out.plain_language_headline=out.plain_language_read;};

 // quiet after activity is the activity fading — a market observation, not a gap in the data
 if(cur.state==='balanced'){
  if(inRun(prev)&&!closing(prev))say('fading',`Earlier ${subjectOf(prev).toLowerCase()} is fading`);
  return out;
 }
 if(!inRun(cur))return out;

 // A NAMED BEHAVIOUR CHANGING IS A NEW RUN. Found live 21 Sep 10:45: "Call writing broadens" at 10:30 was followed by
 // "Call buying narrows" — premium had flipped from falling to rising, and the headline implied call buying had been
 // there all along. Unnamed "activity" is compatible with either.
 const sameRegime=!prev.regime||!cur.regime||prev.regime===cur.regime;
 const sameRun=inRun(prev)&&prev.row===cur.row&&closing(prev)===closing(cur)&&sameRegime;
 // "NEW ACTIVITY" MEANS NEW SINCE THE STORED PREVIOUS SNAPSHOT — not new on this reading's own grid, which may be a
 // different contract set after an ATM move (found live 21 Sep 10:30: 23,350 and 23,450 CE, active at 10:15, were
 // labelled new).
 if(prev.row===cur.row&&inRun(prev)){
  const had=prev.strike_range||[];
  out.key_strikes=(cur.key_strikes||[]).map(k=>({...k,note:k.lead?'Highest activity'
   :had.indexOf(k.strike)<0?'New activity':k.note==='New activity'?(closing(cur)?'Reducing':'Building'):k.note}));
 }
 const code=cur.side||'';
 // POSITIONS BEING CLOSED ARE ALWAYS REVERSING — red, never a green "appeared" or "held". Found live 21 Sep 14:00:
 // "APPEARED · Call short covering appears" over "Call OI fell 81.6L": a reduction painted as a build.
 if(!sameRun){
  if(closing(cur))say('reversing',`${who(cur)} positions being reduced ${cur.location}`);
  else if(prev.state!=='opening')say('appeared',`${subjectOf(cur)} appears ${cur.location}`);
  return out;
 }

 // THE SAME BEHAVIOUR, ON THE SAME SIDE, AGAIN: persistence exists now, and it is time between two stamps
 const since=prev.persistence_since||prev.timestamp;
 out.persistence_since=since;
 out.persistence_minutes=minutesBetween(since,cur.timestamp);
 out.consecutive_readings=(prev.consecutive_readings||1)+1;
 out.persistence_known=true;out.persistence_evidence_available=true;out.persistence_status='established';

 if(closing(cur)){
  say('reversing',`${who(cur)} positions still being reduced ${cur.location}`);
  out.breadth_previous=(prev.strike_range||[]).length;out.breadth_current=(cur.strike_range||[]).length;
  return out;
 }
 const was=prev.strike_range||[],now=cur.strike_range||[];
 const joined=now.filter(k=>was.indexOf(k)<0),left=was.filter(k=>now.indexOf(k)<0);
 // breadth against the STORED previous set — like with like
 out.breadth_previous=was.length;out.breadth_current=now.length;
 out.breadth_direction=now.length>was.length?'wider':now.length<was.length?'narrower':'same';
 out.tags=out.tags.filter(t=>t!=='Broader activity'&&t!=='Narrowing');
 if(out.breadth_direction==='wider')out.tags=['Broader activity',...out.tags].slice(0,3);
 if(out.breadth_direction==='narrower')out.tags=['Narrowing',...out.tags].slice(0,3);

 const span=(l:number[])=>{const s=l.slice().sort((a,b)=>a-b);
  return !s.length?'':s.length===1?`${strikeText(s[0])} ${code}`:`${strikeText(s[0])}–${strikeText(s[s.length-1])} ${code}`;};
 const subj=subjectOf(cur);
 if(joined.length&&!left.length){
  const into=Math.min(...joined)>Math.max(...was)?'higher':Math.max(...joined)<Math.min(...was)?'lower':'more';
  say('broadened',`${subj} broadens into ${into} strikes`);
 } else if(left.length&&!joined.length)say('concentrated',`${subj} narrows to ${span(now)}`);
 else if(prev.leading_strike!=null&&cur.leading_strike!=null&&prev.leading_strike!==cur.leading_strike)
  say('shifted',`Leading ${who(cur).toLowerCase()} activity shifts from ${strikeText(prev.leading_strike)} to ${strikeText(cur.leading_strike)} ${code}`);
 else if(prev.weight>0&&cur.weight>prev.weight*PACE_UP)say('strengthened',`${subj} strengthens ${cur.location}`);
 else if(prev.weight>0&&cur.weight<prev.weight*PACE_DOWN)say('slowed',`${subj} losing momentum ${cur.location}`);
 else say('held',`${subj} holding ${cur.location}`);
 return out;
}

/** A whole session through the chain, oldest first: each reading's own snapshot, joined to the one before it. */
export function chainSession(snapshots:SignalState[]):SignalState[]{
 const out:SignalState[]=[];
 for(const s of snapshots){out.push(chainStep(out.length?out[out.length-1]:null,s));}
 return out;
}

// ================================================================================================================
// COLLAPSING THE QUIET. Consecutive readings in the same state are one period, and nothing is deleted:
// every reading it covers travels with it and the period opens to show them.
// ================================================================================================================
export type Period={
 from:string|null;to:string|null;minutes:number|null;readings:SignalState[];
 state:State;label:string;read:string;detail:string;count:number;latest:boolean;
};

export function periods(list:SignalState[]):Period[]{
 // `list` is newest first; the run is easier to see oldest first, so it is walked forwards and flipped back
 const oldest=list.slice().reverse();
 const out:Period[]=[];
 for(const item of oldest){
  const last=out[out.length-1];
  if(last&&last.state===item.state&&last.readings[0].row===item.row){
   last.readings.push(item);
   last.to=item.timestamp;
   last.count+=1;
   // the period speaks with its NEWEST reading's words: the run's latest state is the one that matters
   last.read=item.plain_language_read;
   last.detail=item.plain_language_detail;
   continue;
  }
  out.push({from:item.previous_timestamp||item.timestamp,to:item.timestamp,minutes:null,
   readings:[item],state:item.state,label:STATE_LABEL[item.state],
   read:item.plain_language_read,detail:item.plain_language_detail,count:1,latest:false});
 }
 for(const period of out){
  // the period spans from the reading BEFORE its first entry to its last: that is the ground it covers
  period.minutes=period.count*15;
 }
 const flipped=out.reverse();
 if(flipped.length)flipped[0].latest=true;
 return flipped;
}

// ================================================================================================================
// HOW TO READ THIS — every colour, every word, in one place. Nothing explanatory is printed below the pane.
// ================================================================================================================
export const SIGNAL_GUIDE=[
 {heading:'What this pane is',lines:[
  'The latest 15-minute reading, and the states the session moved through before it. Every change is '
  +'measured against the reading immediately before, and both ends of that interval are named.',
  'It describes what the option book DID. It is not a recommendation and it names no buyer or seller.',
 ]},
 {heading:'The state words',lines:[
  'OPENING READ — the first reading of the session: where call and put open interest are concentrated near the '
  +'money. There is no earlier reading, so nothing is said about change.',
  'BUILDING — positions are being added on the side the reading is about. APPEARED — the behaviour was not '
  +'there at the reading before. HELD — the same behaviour, continuing (the headline says holding).',
  'STRENGTHENED · SLOWED — the same behaviour, moving faster or slower than the interval before (the '
  +'headline says losing momentum).',
  'BROADENED · CONCENTRATED — more strikes joined, or the set narrowed.',
  'SHIFTED — the strike carrying the largest change moved.',
  'FADING — it stopped registering. REVERSING — positions are being closed rather than opened.',
  'BALANCED — no material change in market structure. MIXED — one side opening positions while the other '
  +'closes them, at a comparable scale. Both sides adding positions is not MIXED: the headline names the '
  +'larger side and the evidence names both.',
  'NOT OBSERVED — the reading was taken but carried no comparable value. That is an absence of measurement, '
  +'not a quiet market.',
 ]},
 {heading:'Persistence',lines:[
  '"Since 10:30" appears once the same behaviour has held across more than one comparable reading. Until '
  +'then there is no persistence to report, and none is shown.',
  'It is time between two stamps, never a score: readings at 09:30, 09:45 and 10:00 are THIRTY minutes.',
  'Early in the session each state is read from the latest fifteen minutes; persistence joins it once the '
  +'behaviour has repeated.',
 ]},
 {heading:'The colours',lines:[
  'GREEN — increasing, building, expanding.',
  'RED — decreasing, weakening, contracting.',
  'GREY — unchanged or stable.',
  'AMBER — slowing, or evidence that conflicts or is missing.',
  {text:'No colour here means bullish or bearish. Rising call open interest is shown as an increase '
   +'because it IS one — it is not painted as good news.',tone:'amber' as const},
 ]},
 {heading:'The five metrics',lines:[
  'PRICE is the premium of the single contract that moved most this interval — a real, tradeable '
  +'instrument. Five strikes added together are not one, so their sum is never shown as a price.',
  'Δ OPEN INTEREST is the change across the five at-the-money contracts of the side carrying the reading.',
  'Δ IV is at-the-money volatility in POINTS. It is solved from premium and spot rather than reported by '
  +'the exchange; where it could not be solved the box says so instead of showing a zero.',
  'Δ PCR is put open interest against call open interest, across the whole book.',
  'MAX PAIN is the strike at which the most open interest would expire worthless. Context, not a target, '
  +'and the weakest of the five.',
 ]},
 {heading:'Evidence and conflict',lines:[
  'A behaviour — writing, buying, covering — is an INTERPRETATION of open interest and premium read '
  +'together. It is never shown without what contradicts it or fails to confirm it.',
  {text:'Open interest and premium cannot identify who initiated a trade: every contract opened has a '
   +'buyer and a seller. The underlying’s own move affects every premium, so a falling premium is not by '
   +'itself evidence of anything.',tone:'amber' as const},
 ]},
 {heading:'Quiet periods',lines:[
  'Consecutive readings in the same state are shown as ONE period with the number of readings it covers. '
  +'Nothing is deleted — open the period, or use "Show all readings", to see each one.',
  'A quiet session is allowed to look quiet. The pane will not invent activity to fill itself.',
 ]},
];

// ================================================================================================================
// WHERE POSITIONS STAND — what ONE reading can say on its own.
//
// Asked for live on 21 Sep 2026, at the day's first reading, when the middle and right panes both read "not
// enough readings yet": one reading cannot say what CHANGED, but it says in full where the book STANDS — which
// call and put strikes hold the most open interest, where spot sits between them, the put/call ratio and max
// pain. Those are LEVELS, observed at one stamp. Nothing here is a fifteen-minute change, and the only
// comparison it makes is against the previous session's close, which is named as such every time.
//
// The same discipline as everything else in this file: it names where open interest sits, never who holds it
// or why. A heavy call strike is heavy; whether it is a ceiling is a reading the trader brings, not a fact the
// store holds, and the guide says so.
// ================================================================================================================
export type StandingStrike={strike:number;oi:number;share:number|null;distance:number|null};
export type Standing={
 at:string|null;expiry:string|null;days_to_expiry:number|null;spot:number|null;atm:number|null;
 calls:StandingStrike[];puts:StandingStrike[];
 total_ce_oi:number|null;total_pe_oi:number|null;pcr:number|null;
 max_pain:number|null;max_pain_distance:number|null;
 day_calls:{strike:number;change:number}[];day_puts:{strike:number;change:number}[];
 headline:string;lines:string[];caveat:string;
 /** THE OPENING READ, on the SAME near-the-money set the grid is built on (ATM and four strikes up for calls,
  *  ATM and four down for puts), so the first reading and every reading after it describe one set of contracts. */
 opening:string;open_calls:number[];open_puts:number[];
};

// Only what is true at EVERY reading this appears at. "A second reading is needed" was printed at 09:45 on 21 Sep
// beside a middle pane showing that very 15-minute change — so the part about change is said by the caller,
// which knows whether a second reading exists.
export const STANDING_CAVEAT='These are levels at one reading. Open interest says where positions sit, not '
 +'who opened them or why.';

export function standing(body:any,top=3):Standing|null{
 const rows:any[]=Array.isArray(body?.rows)?body.rows:[];
 if(!rows.length)return null;
 const spot=num(body?.spot);
 const strikes=rows.map(r=>num(r.strike)).filter((v):v is number=>v!=null);
 const atm=spot==null||!strikes.length?null:strikes.reduce((b,s)=>Math.abs(s-spot)<Math.abs(b-spot)?s:b,strikes[0]);
 const totalCe=num(body?.total_ce_oi),totalPe=num(body?.total_pe_oi);
 // the heaviest strikes on each side. A strike with no captured OI is skipped, never ranked as a zero.
 const rank=(key:'ce_oi'|'pe_oi',total:number|null):StandingStrike[]=>rows
  .map(r=>({strike:num(r.strike),oi:num(r[key])}))
  .filter((r):r is {strike:number;oi:number}=>r.strike!=null&&r.oi!=null&&r.oi>0)
  .sort((a,b)=>b.oi-a.oi).slice(0,top)
  .map(r=>({...r,share:total?round(r.oi/total*100,0):null,distance:spot==null?null:round(r.strike-spot,2)}));
 const calls=rank('ce_oi',totalCe),puts=rank('pe_oi',totalPe);
 if(!calls.length&&!puts.length)return null;
 const dayRank=(key:'ce_oi_change_day'|'pe_oi_change_day')=>rows
  .map(r=>({strike:num(r.strike),change:num(r[key])}))
  .filter((r):r is {strike:number;change:number}=>r.strike!=null&&r.change!=null&&r.change>0)
  .sort((a,b)=>b.change-a.change).slice(0,1);
 const dayCalls=dayRank('ce_oi_change_day'),dayPuts=dayRank('pe_oi_change_day');
 const pcr=totalCe&&totalPe!=null?round(totalPe/totalCe,2):null;
 const maxPain=num(body?.max_pain_strike);
 const mpDist=maxPain!=null&&spot!=null?round(maxPain-spot,2):null;

 const pts=(v:number)=>`${Math.abs(v).toLocaleString('en-IN',{maximumFractionDigits:2})} points`;
 const c0=calls[0],p0=puts[0];
 const headline=c0&&p0
  ?`Call open interest is heaviest at ${strikeText(c0.strike)} CE, put open interest at ${strikeText(p0.strike)} PE`
  :c0?`Call open interest is heaviest at ${strikeText(c0.strike)} CE`
  :`Put open interest is heaviest at ${strikeText(p0!.strike)} PE`;
 const lines:string[]=[];
 if(spot!=null&&c0&&p0){
  const between=c0.strike>=spot&&p0.strike<=spot;
  lines.push(between
   ?`Spot ${RUPEE}${spot.toLocaleString('en-IN',{maximumFractionDigits:2})} sits between them: `
    +`${pts(c0.strike-spot)} below the heaviest call strike and ${pts(spot-p0.strike)} above the heaviest put strike.`
   :`Spot is ${RUPEE}${spot.toLocaleString('en-IN',{maximumFractionDigits:2})}; the heaviest call strike is `
    +`${c0.strike>=spot?`${pts(c0.strike-spot)} above`:`${pts(spot-c0.strike)} below`} it and the heaviest put strike `
    +`${p0.strike<=spot?`${pts(spot-p0.strike)} below`:`${pts(p0.strike-spot)} above`} it.`);
 }
 const parts:string[]=[];
 if(pcr!=null)parts.push(`the put/call open-interest ratio is ${pcr.toFixed(2)} for this expiry`);
 if(maxPain!=null)parts.push(`max pain is ${strikeText(maxPain)}${mpDist==null?''
  :mpDist===0?', at spot':`, ${pts(mpDist)} ${mpDist>0?'above':'below'} spot`}`);
 if(parts.length)lines.push(parts.join('; ').replace(/^./,c=>c.toUpperCase())+'.');
 if(dayCalls.length||dayPuts.length){
  const bits:string[]=[];
  if(dayCalls.length)bits.push(`call open interest was added most at ${strikeText(dayCalls[0].strike)} CE (+${compact(dayCalls[0].change)})`);
  if(dayPuts.length)bits.push(`put open interest at ${strikeText(dayPuts[0].strike)} PE (+${compact(dayPuts[0].change)})`);
  lines.push(`Since the previous session's close, ${bits.join(' and ')}.`);
 }
 const ladder=strikes.slice().sort((a,b)=>a-b);
 const ai=atm==null?-1:ladder.indexOf(atm);
 const window=(r:'ce_oi'|'pe_oi')=>{
  if(ai<0)return [] as number[];
  const set=r==='ce_oi'?ladder.slice(ai,ai+5):ladder.slice(Math.max(0,ai-4),ai+1);
  return rows.filter(x=>set.indexOf(num(x.strike) as number)>=0&&num(x[r])!=null&&(num(x[r]) as number)>0)
   .sort((a,b)=>(num(b[r]) as number)-(num(a[r]) as number)).slice(0,3).map(x=>num(x.strike) as number).sort((a,b)=>a-b);
 };
 const openCalls=window('ce_oi'),openPuts=window('pe_oi');
 const rng=(l:number[],c:string)=>!l.length?'':l.length===1?`${strikeText(l[0])} ${c}`:`${strikeText(l[0])}–${strikeText(l[l.length-1])} ${c}`;
 const opening=[openCalls.length?`Calls concentrated around ${rng(openCalls,'CE')}`:'',
  openPuts.length?`${openCalls.length?'puts':'Puts'} around ${rng(openPuts,'PE')}`:''].filter(Boolean).join('; ');
 return {opening,open_calls:openCalls,open_puts:openPuts,
  at:body?.as_of||null,expiry:body?.expiry||null,days_to_expiry:num(body?.days_to_expiry),spot,atm,
  calls,puts,total_ce_oi:totalCe,total_pe_oi:totalPe,pcr,max_pain:maxPain,max_pain_distance:mpDist,
  day_calls:dayCalls,day_puts:dayPuts,headline,lines,caveat:STANDING_CAVEAT};
}

/** THE SESSION'S FIRST READING AS A SNAPSHOT. There is no earlier reading, so there is no grid to walk: the
 *  opening read comes from the near-the-money open interest AT this reading (oi-by-strike, same ATM rule as the
 *  grid), and every change field is null — nothing is claimed about change. */
export function openingSnapshot(body:any,at:string):SignalState|null{
 const s=standing(body);
 if(!s||!s.opening)return null;
 const none=null;
 return {
  timestamp:at,previous_timestamp:none,current_timestamp:at,interval_minutes:none,
  state:'opening',state_change:STATE_LABEL.opening,market_state:'opening',call_state:'opening',put_state:'opening',
  side:s.open_calls.length?'CE':'PE',row:s.open_calls.length?'calls':'puts',
  location:'',strike_range:s.open_calls.slice(),strike_cluster:s.open_calls.slice(),leading_strike:none,
  persistence_since:none,persistence_minutes:none,consecutive_readings:0,persistence_known:false,
  persistence_evidence_available:false,persistence_status:'none',
  breadth_previous:none,breadth_current:0,breadth_direction:'none',breadth:'none',
  price_change:none,price_change_pct:none,price_level:none,price_contract:'',
  call_oi_change:none,put_oi_change:none,call_oi_level:s.total_ce_oi,put_oi_level:s.total_pe_oi,
  call_iv_change:none,put_iv_change:none,iv_change:none,iv_level:none,iv_reason:'',
  pcr_previous:none,pcr_current:s.pcr,pcr_change:none,
  max_pain_previous:none,max_pain_current:s.max_pain,max_pain_change:none,
  supporting_evidence:[],conflicting_evidence:[],opposing_side_state:'opening',
  regime:none,tags:[],key_strikes:[],
  plain_language_read:s.opening,plain_language_detail:'',plain_language_headline:s.opening,plain_language_evidence:'',
  behaviour:'none',weight:0,engine_version:ENGINE_VERSION,rules_version:SIGNAL_RULE_VERSION,
  covered:true,
 };
}
