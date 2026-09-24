// BLOCK 1 — the selected instrument's evolving explanation, in plain language.
//
// STRUCTURED OBSERVATIONS FIRST, LANGUAGE SECOND. Nothing in this file invents an analytic. Every behaviour
// word comes out of `signalSide` — the tab's own verified rule (FLOW_LABELS keyed on price direction AND open
// interest direction together, rule `signal/2`) — applied to ONE contract's slot instead of a whole side. The
// window, the flat bands and the strength ladder are the ones already in force; this file adds no threshold.
//
// WHAT IT ADDS is the part the tab had nowhere to put: which STRIKES are doing it, whether that set grew or
// shrank since the reading before, how long the behaviour has been running, and the sentences that say so.
//
// THREE RULES IT KEEPS TO, because a sentence is harder to audit than a number:
//
//  1. NOTHING AFTER THE SELECTED READING IS READ. The walk is forward and each reading is built from readings
//     0..i only, exactly as `signalRows` does. Truncating the points is what makes history stop repainting.
//  2. AN UNOBSERVED INTERVAL IS NOT A MEASURED ONE. A reading that carried no comparable value breaks the run:
//     the elapsed time freezes at the last reading that DID carry one, and it is never counted forward.
//  3. A CLAUSE WITH NO FIELD BEHIND IT IS NOT WRITTEN. Implied volatility is not in this payload, so no
//     sentence here mentions it; the strike set is described only from readings that carried a value.
import {DASH,MINUS,NO_DIRECTION,RUPEE,SIGNAL_RULE_VERSION,SIGNAL_WINDOW_MAX_MINUTES,SIGNAL_WINDOW_MINUTES,
 clock,compact,readingMinutes,signalBaselineIndex,signalSide,strike as strikeText,
 type SignalSide} from './logic';
import type {GridSlot,OiGrid} from './types';

/** What one contract appears to be doing, named from the pair the tab already reads together. These are the
 *  FLOW_LABELS cells, grouped: the four that name a participant, the two that name a position change without
 *  one, and the three that name no change at all. */
export type Behaviour='buying'|'writing'|'short_covering'|'buyers_exiting'
 |'positions_added'|'positions_closing'|'price_only'|'quiet'|'none';
/** The four that carry a story. The other five are true and are reported, but they do not start a run. */
export const DIRECTIONAL:Behaviour[]=['buying','writing','short_covering','buyers_exiting'];
/** The two that are an EXIT rather than an entry — what "unwound" is built on. */
const EXITS:Behaviour[]=['short_covering','buyers_exiting'];

/** How the story moved between this reading and the one before it. */
export type StoryState='appeared'|'continued'|'broadened'|'narrowed'|'shifted'|'strengthened'|'slowed'
 |'unwound'|'faded'|'quiet'|'not_observed'|'no_baseline';

/** The pace bands. They are the BLOCK's own ratio, not a new constant: a change more than a quarter larger
 *  than the reading before is strengthening, more than a quarter smaller is slowing, and the band between is
 *  the same behaviour continuing. */
export const PACE_UP=1.25,PACE_DOWN=0.75;

export type StrikeReading={
 strike:number|null;label:string;side:'CE'|'PE';row:'calls'|'puts';
 oi_change:number|null;price_change:number|null;
 oi_direction:string;price_direction:string;
 what_label:string;meaning:string;behaviour:Behaviour;directional:boolean;
};
export type SideReading={
 side:'CE'|'PE';row:'calls'|'puts';
 behaviour:Behaviour;
 /** Every strike reading this behaviour at this reading, low to high. */
 strikes:number[];
 /** The strike with the largest position change among them — the busiest, in the reader's words. */
 lead:number|null;lead_oi_change:number|null;lead_price_change:number|null;
 /** The strikes that joined, and the strikes that dropped out, since the reading immediately before. */
 joined:number[];left:number[];
 /** The sum of |position change| over the strikes above — the pace the bands are read on. */
 weight:number;
 /** True only when the SAME strike has carried the largest addition at every reading of this run. Without
  *  it, "23,300 has led throughout" is a claim about intervals nobody checked. */
 lead_stable:boolean;
 /** The side aggregate the signal table already shows, unchanged, so the two can never disagree. */
 total:SignalSide|null;
 state:StoryState;
 /** The first reading of the unbroken run this reading belongs to, and how long that run has run FOR. */
 first_at:string|null;elapsed_minutes:number|null;scans:number;
 /** The last reading that actually CARRIED a value for this run — what "last confirmed" means. */
 last_confirmed_at:string|null;
 /** How many of this side's five slots carried a comparable value here. */
 measured:number;slots:number;
};
/** One rung of the ladder the panel draws: what this strike did BETWEEN THE LAST TWO READINGS, and — just
 *  as important — which kind of nothing it did when it did nothing. A rung that was measured and did not
 *  move is a fact; a rung the capture never reached is an absence, and one mark cannot mean both. */
export type Rung={
 strike:number;side:'CE'|'PE';row:'calls'|'puts';
 oi_added:number|null;price_change:number|null;
 /** What the contract itself HELD at this reading — the value behind the change. */
 oi_level:number|null;price_level:number|null;
 status:'added'|'reduced'|'no_change'|'not_captured';
 /** Set when this rung joined the current run AT THIS READING. */
 joined:boolean;
 /** The reading this rung has been in the run since, when it is in one. */
 since:string|null;
};
export type ReadingObservation={
 at:string|null;
 /** The reading the EPISODE was measured against, and how wide that comparison actually was. */
 from:string|null;window_minutes:number|null;
 /** The reading immediately before this one. Every "what just changed" statement is measured against THIS
  *  one, and prints both ends — a 15-minute product that only knows an hour-wide window cannot say what
  *  happened in the last fifteen minutes. */
 previous_at:string|null;
 covered:boolean;
 /** The episode, on the tab's existing hour-wide window. */
 calls:SideReading;puts:SideReading;
 /** The same two sides read against the PREVIOUS READING only. Noisier by construction, which is why
  *  nothing built on it is allowed to name a behaviour in a headline. */
 calls_interval:SideReading|null;puts_interval:SideReading|null;
 strikes:StrikeReading[];
 /** The ladder, lead side first, at this reading. */
 ladder:Rung[];
 rule_version:string;
};

/** ABSENCE IS NOT ZERO. Number(null) is 0 and finite, so a bare Number() coercion turns a field that
 *  was never captured into a captured nought — which is the one mistake this whole tab exists to avoid.
 *  Nulls and blanks are checked BEFORE the coercion, every time. */
const num=(v:unknown)=>{
 if(v==null||v==='')return null;
 const n=typeof v==='number'?v:Number(v);
 return Number.isFinite(n)?n:null;
};

/** One contract's behaviour, from the tab's own table. `signalSide` sums over the slots it is handed, so a
 *  one-slot basket IS that contract — the same rule, the same bands, no second implementation. */
export function behaviourOf(kind:'CE'|'PE',priceDirection:unknown,oiDirection:unknown):Behaviour{
 const p=String(priceDirection||''),o=String(oiDirection||'');
 if(p===NO_DIRECTION||o===NO_DIRECTION||!p||!o)return 'none';
 if(o==='building')return p==='up'?'buying':p==='down'?'writing':'positions_added';
 if(o==='unwinding')return p==='up'?'short_covering':p==='down'?'buyers_exiting':'positions_closing';
 return p==='flat'?'quiet':'price_only';
}

/** Low to high, de-duplicated, nulls dropped. */
const ladder=(list:(number|null)[])=>Array.from(new Set(list.filter((v):v is number=>v!=null))).sort((a,b)=>a-b);

/** The side's behaviour at one reading: the one the most strikes are reading, and — when two tie — the one
 *  carrying the larger total position change. A tie that survives BOTH tests is not resolved; the side is
 *  reported as reading nothing in particular rather than being handed to whichever came first. */
function sideBehaviour(rows:StrikeReading[]):{behaviour:Behaviour;strikes:number[]}{
 const bag=new Map<Behaviour,{n:number;weight:number;strikes:(number|null)[]}>();
 for(const row of rows){
  if(!row.directional)continue;
  const cur=bag.get(row.behaviour)||{n:0,weight:0,strikes:[] as (number|null)[]};
  cur.n+=1;cur.weight+=Math.abs(num(row.oi_change)||0);cur.strikes.push(row.strike);
  bag.set(row.behaviour,cur);
 }
 if(!bag.size)return {behaviour:'none',strikes:[]};
 const ranked=[...bag.entries()].sort((a,b)=>b[1].n-a[1].n||b[1].weight-a[1].weight);
 if(ranked.length>1&&ranked[0][1].n===ranked[1][1].n&&ranked[0][1].weight===ranked[1][1].weight)
  return {behaviour:'none',strikes:[]};
 return {behaviour:ranked[0][0],strikes:ladder(ranked[0][1].strikes)};
}

type Run={behaviour:Behaviour;first:string|null;last:string|null;scans:number;strikes:number[];
 lead:number|null;weight:number;leadStable:boolean};

/** THE FORWARD WALK. One entry per reading the grid holds, built from that reading and the ones before it.
 *
 *  `upto` truncates the session to the reading on screen. It is the whole of rule 1: hand this the full grid
 *  and the last entry describes the newest reading; hand it the selected one and the last entry describes
 *  what was knowable THEN, with no later reading anywhere in the arithmetic. */
export function observe(grid?:OiGrid|null,upto?:string|null,
 window=SIGNAL_WINDOW_MINUTES,limit=SIGNAL_WINDOW_MAX_MINUTES):ReadingObservation[]{
 const slots=((grid?.rows||[]) as GridSlot[]).filter(s=>s&&s.present);
 if(!slots.length)return [];
 const calls=slots.filter(s=>s.row!=='puts'),puts=slots.filter(s=>s.row==='puts');
 const length=Math.max(0,...slots.map(s=>(s.points||[]).length));
 if(!length)return [];
 const times=Array.from({length},(_,i)=>slots.map(s=>s.points?.[i]?.at).find(Boolean)||null);
 // THE BOUNDARY. A reading later than the one on screen is not walked at all.
 const bound=readingMinutes(upto);
 const last=bound==null?length-1:times.reduce((keep,at,i)=>{
  const m=readingMinutes(at);return m!=null&&m<=bound?i:keep;},-1);
 if(last<0)return [];

 const state={calls:{peak:0,pricePeak:0,ranked:0},puts:{peak:0,pricePeak:0,ranked:0}};
 // THE INTERVAL KEEPS ITS OWN YARDSTICK. The flat band is a fraction of the side’s largest move SO FAR,
 // and an hour-wide move is far larger than a fifteen-minute one — judge the interval against the hour and
 // almost every interval reads as flat. Two windows, two peaks.
 const istate={calls:{peak:0,pricePeak:0,ranked:0},puts:{peak:0,pricePeak:0,ranked:0}};
 const runs:Record<'calls'|'puts',Run|null>={calls:null,puts:null};
 const before:Record<'calls'|'puts',SideReading|null>={calls:null,puts:null};
 const out:ReadingObservation[]=[];

 for(let i=0;i<=last;i++){
  const at=times[i];
  const back=signalBaselineIndex(times,i,window,limit);
  const from=back==null?null:times[back];
  const span=back==null?null:(readingMinutes(at)??0)-(readingMinutes(from)??0);
  const strikeRows:StrikeReading[]=[];

  const side=(list:GridSlot[],kind:'CE'|'PE',row:'calls'|'puts',keep:{peak:number;pricePeak:number;ranked:number}):SideReading=>{
   // the side aggregate FIRST, on the very state machine the signal table advances, so the panel and the
   // table are two readings of one number rather than two numbers
   const own={...keep};
   let oiChange:number|null=null,priceChange:number|null=null;
   if(back!=null){
    const total=signalSide(list,i,back,kind,own.peak,own.pricePeak,own.ranked);
    oiChange=total?total.change:null;priceChange=total?total.price_change:null;
   }
   const peak=oiChange==null?own.peak:Math.max(own.peak,Math.abs(oiChange));
   const pricePeak=priceChange==null?own.pricePeak:Math.max(own.pricePeak,Math.abs(priceChange));
   const ranked=own.ranked+(oiChange==null?0:1);
   const total=signalSide(list,i,back,kind,peak,pricePeak,ranked);
   keep.peak=peak;keep.pricePeak=pricePeak;keep.ranked=ranked;

   // then each contract on its own, through the SAME function with a one-slot basket
   let measured=0;
   const rows:StrikeReading[]=list.map(slot=>{
    const one=back==null?null:signalSide([slot],i,back,kind,peak,pricePeak,ranked);
    if(one)measured+=1;
    const behaviour=one?behaviourOf(kind,one.price_direction,one.oi_direction):'none';
    return {strike:num(slot.strike),label:slot.label||'',side:kind,row,
     oi_change:one?one.change:null,price_change:one?one.price_change:null,
     oi_direction:one?one.oi_direction:NO_DIRECTION,price_direction:one?one.price_direction:NO_DIRECTION,
     what_label:one?one.what_label:'',meaning:one?one.meaning:'',
     behaviour,directional:DIRECTIONAL.indexOf(behaviour)>=0};
   });
   strikeRows.push(...rows);

   const picked=sideBehaviour(rows);
   const active=rows.filter(r=>r.directional&&r.behaviour===picked.behaviour&&r.strike!=null);
   const lead=active.reduce<StrikeReading|null>((best,r)=>
    !best||Math.abs(num(r.oi_change)||0)>Math.abs(num(best.oi_change)||0)?r:best,null);
   const weight=active.reduce((sum,r)=>sum+Math.abs(num(r.oi_change)||0),0);

   const prev=before[row];
   const joined=prev?picked.strikes.filter(s=>prev.strikes.indexOf(s)<0):[];
   const left=prev?prev.strikes.filter(s=>picked.strikes.indexOf(s)<0):[];

   // RULE 2. A reading with no comparable value is not a quiet market and not a break in the market — it is
   // an absence of measurement. The run is neither extended nor ended by it.
   const covered=measured>0;
   let run=runs[row];
   let stateWord:StoryState;
   if(back==null){stateWord='no_baseline';}
   else if(!covered){stateWord='not_observed';}
   else if(picked.behaviour==='none'){
    stateWord=run?'faded':'quiet';
    run=null;
   } else if(!run||run.behaviour!==picked.behaviour){
    // an EXIT that follows an entry is the story turning, not a new one starting
    stateWord=run&&EXITS.indexOf(picked.behaviour)>=0&&DIRECTIONAL.indexOf(run.behaviour)>=0
     &&EXITS.indexOf(run.behaviour)<0?'unwound':'appeared';
    run={behaviour:picked.behaviour,first:at,last:at,scans:1,strikes:picked.strikes,
     lead:lead?lead.strike:null,weight,leadStable:true};
   } else {
    const grew=joined.length>0&&left.length===0;
    const shrank=left.length>0&&joined.length===0;
    const moved=lead&&run.lead!=null&&lead.strike!=null&&lead.strike!==run.lead;
    stateWord=grew?'broadened':shrank?'narrowed':moved?'shifted'
     :run.weight>0&&weight>run.weight*PACE_UP?'strengthened'
     :run.weight>0&&weight<run.weight*PACE_DOWN?'slowed':'continued';
    run={behaviour:run.behaviour,first:run.first,last:at,scans:run.scans+1,strikes:picked.strikes,
     lead:lead?lead.strike:run.lead,weight,
     // the claim 'X has led throughout' is only allowed while this stays true
     leadStable:run.leadStable&&(!lead||lead.strike==null||lead.strike===run.lead)};
   }
   runs[row]=run;

   // RULE 2 AGAIN, and this is where it would leak if it were going to. The elapsed time is measured to the
   // last reading that CARRIED a value — `run.last`, which only a covered reading advances — and never to the
   // reading on screen. A capture that stopped at 10:45 and resumed at 11:30 reports the 30 minutes it
   // measured, not the 75 minutes that passed.
   const first=run?run.first:null;
   const startedAt=readingMinutes(first),here=readingMinutes(run?run.last:at);
   const elapsed=run&&startedAt!=null&&here!=null?here-startedAt:null;
   const built:SideReading={side:kind,row,behaviour:picked.behaviour,strikes:picked.strikes,
    lead:lead?lead.strike:null,lead_oi_change:lead?lead.oi_change:null,
    lead_price_change:lead?lead.price_change:null,
    joined,left,weight,total,state:stateWord,
    lead_stable:!!run&&run.leadStable,
    first_at:first,last_confirmed_at:run?run.last:null,elapsed_minutes:elapsed,scans:run?run.scans:0,
    measured,slots:list.length};
   before[row]=built;
   return built;
  };

  const c=side(calls,'CE','calls',state.calls),p=side(puts,'PE','puts',state.puts);

  // THE INTERVAL, read against the reading immediately before. Same function, same bands, a different and
  // NARROWER window — which is the window a fifteen-minute product is actually about. It is noisier than the
  // hour, and that is exactly why nothing built on it may name a behaviour in a headline.
  const prev=i>0?i-1:null;
  const oneStep=(list:GridSlot[],kind:'CE'|'PE',keep:{peak:number;pricePeak:number;ranked:number})=>{
   if(prev==null)return null;
   const first=signalSide(list,i,prev,kind,keep.peak,keep.pricePeak,keep.ranked);
   const oi=first?first.change:null,pr=first?first.price_change:null;
   keep.peak=oi==null?keep.peak:Math.max(keep.peak,Math.abs(oi));
   keep.pricePeak=pr==null?keep.pricePeak:Math.max(keep.pricePeak,Math.abs(pr));
   keep.ranked+=oi==null?0:1;
   return signalSide(list,i,prev,kind,keep.peak,keep.pricePeak,keep.ranked);
  };
  const ci=oneStep(calls,'CE',istate.calls),pi=oneStep(puts,'PE',istate.puts);
  const asSide=(v:SignalSide|null,kind:'CE'|'PE',row:'calls'|'puts'):SideReading|null=>v?{
   side:kind,row,behaviour:behaviourOf(kind,v.price_direction,v.oi_direction),strikes:[],
   lead:null,lead_oi_change:v.change,lead_price_change:v.price_change,joined:[],left:[],
   weight:Math.abs(num(v.change)||0),lead_stable:false,total:v,state:'continued',
   first_at:null,last_confirmed_at:null,elapsed_minutes:null,scans:0,measured:v.contracts,slots:list_len(row),
  }:null;
  function list_len(row:'calls'|'puts'){return row==='puts'?puts.length:calls.length;}

  // THE LADDER, measured over that same one-step window so the bars and the sentence share a window.
  // THE LADDER, over the same one-step window the sentence above is measured over, for BOTH sides. The
  // right-hand explanation draws only the side its story is about and filters by ; the middle pane
  // needs a call rung and a put rung on the same line, and neither should walk this twice.
  const rungOf=(slot:GridSlot,kind:'CE'|'PE',row:'calls'|'puts',keep:{peak:number;pricePeak:number;ranked:number},inRun:SideReading):Rung=>{
   const k=num(slot.strike);
   const one=prev==null?null:signalSide([slot],i,prev,kind,keep.peak,keep.pricePeak,keep.ranked);
   // NOT num(): Number(null) is 0 and finite, so a missing measurement would read as a captured zero —
   // which is the one thing this rung exists to tell apart. The absence is checked before the coercion.
   const here=slot.points?.[i];
   const delta=here&&here.delta_oi!=null?num(here.delta_oi):null;
   const status:Rung['status']=delta==null?'not_captured'
    :one==null?'no_change'
    :one.oi_direction==='building'?'added':one.oi_direction==='unwinding'?'reduced':'no_change';
   return {strike:k==null?0:k,side:kind,row,
    oi_added:one?one.change:null,price_change:one?one.price_change:null,
    oi_level:here&&here.oi!=null?num(here.oi):null,
    price_level:here&&here.price!=null?num(here.price):null,status,
    joined:k!=null&&inRun.joined.indexOf(k)>=0,
    since:k!=null&&inRun.strikes.indexOf(k)>=0?inRun.first_at:null};
  };
  const rungs:Rung[]=[
   ...calls.map(slot=>rungOf(slot,'CE','calls',istate.calls,c)),
   ...puts.map(slot=>rungOf(slot,'PE','puts',istate.puts,p)),
  ].filter(r=>r.strike>0);

  out.push({at,from,window_minutes:span,previous_at:prev==null?null:times[prev],
   covered:c.measured>0||p.measured>0,calls:c,puts:p,
   calls_interval:asSide(ci,'CE','calls'),puts_interval:asSide(pi,'PE','puts'),
   strikes:strikeRows,ladder:rungs,
   rule_version:SIGNAL_RULE_VERSION});
 }
 return out;
}

// ================================================================================================================
// LANGUAGE — AND THE CLAIM POLICY IT WORKS TO.
//
// Three tiers, each with a data precondition. The tier decides where a sentence is allowed to appear, so an
// overconfident line cannot reach a headline and be "repaired" by a caveat underneath it.
//
//   TIER 1 · OBSERVED      what the measured numbers did, and nothing else.
//                          "Call positions build at another strike."
//                          Precondition: the field was measured at both readings.
//                          ALLOWED: headlines, ladder labels, the session paragraph. Everywhere.
//
//   TIER 2 · QUALIFIED     a named behaviour, offered as an interpretation, WITH the competing explanation
//                          in the same sentence.
//                          "Consistent with writing, though spot also fell 18 points over the same interval."
//                          Precondition: every supporting field present AND the underlying's own move stated.
//                          ALLOWED: the body only. Never a headline, never a screener row.
//
//   TIER 3 · ATTRIBUTED    a bare behaviour claim — "call writing at 23,450".
//                          Precondition: validated price attribution and calibration.
//                          NOT AVAILABLE. Nothing in this file may emit one.
//
// WHY TIER 3 IS CLOSED. The rule this file reads — premium direction and open-interest direction together —
// does not control for the underlying's own move. A call's premium falls when spot falls, whoever is trading,
// so "open interest up, premium down" on a falling market is consistent with writing AND with nothing at all.
// Until the premium move can be measured against what spot alone would have produced, naming the participant
// is an interpretation, and it is written as one. A delta-adjusted residual will improve that; it will not
// close it, because Greeks are estimates and an unexplained residual still does not say who initiated a trade.
// ================================================================================================================

/** "at 23,350 CE" · "across 23,350–23,450 CE" · "at 23,350 and 23,450 CE". A run of strikes that are adjacent
 *  ON THE LADDER SHOWN reads as a range; a set with a hole in it is listed, because "across" would claim the
 *  strike in the middle is in it. */
export function whereText(strikes:number[],side:'CE'|'PE',all:number[]=[]):string{
 const list=ladder(strikes);
 if(!list.length)return '';
 if(list.length===1)return `at ${strikeText(list[0])} ${side}`;
 const rungs=ladder(all.length?all:list);
 const lo=rungs.indexOf(list[0]),hi=rungs.indexOf(list[list.length-1]);
 const contiguous=lo>=0&&hi>=0&&hi-lo===list.length-1;
 if(contiguous)return `across ${strikeText(list[0])}–${strikeText(list[list.length-1])} ${side}`;
 const heads=list.slice(0,-1).map(s=>strikeText(s)).join(', ');
 return `at ${heads} and ${strikeText(list[list.length-1])} ${side}`;
}

const SIDE_WORD:Record<'calls'|'puts',string>={calls:'Call',puts:'Put'};
/** The behaviour words. They may ONLY be used inside `qualified()`, which welds each one to its competing
 *  explanation. Nothing else in this file reads this table. */
const BEHAVIOUR_WORD:Record<Behaviour,string>={
 buying:'buying',writing:'writing',short_covering:'short covering',buyers_exiting:'buyers closing out',
 positions_added:'positions added',positions_closing:'positions closed',price_only:'a premium move',
 quiet:'little change',none:'no clear behaviour',
};

export function minutesText(m?:number|null){
 if(m==null||m<=0)return '';
 if(m<60)return `${m} minutes`;
 const h=Math.floor(m/60),rest=m%60;
 return `${h} hour${h===1?'':'s'}${rest?` ${rest} minutes`:''}`;
}

export type CrossRow={label:string;text:string};
export type Narrative={
 /** TIER 1. What the numbers did over the interval named in `window`. */
 headline:string;
 window:string;
 /** TIER 1. The measured change, and the underlying's own move over the same interval. */
 observed:string;
 /** TIER 2, or '' when the preconditions are not met. Always carries its competing explanation. */
 qualified:string;
 /** TIER 1. How the episode has developed since it began. */
 session:string;
 /** The one-line put (or call) comparison. Never repeated elsewhere. */
 otherSide:string;
 context:string;
 cross:CrossRow[];
 evidence:string[];
 caveat:string;
 cited:number[];
 ladder:Rung[];
 limited:boolean;
};

const money=(v:number|null|undefined)=>{
 const n=num(v);
 if(n==null)return DASH;
 return `${n>0?'+':n<0?MINUS:''}${RUPEE}${Math.abs(n).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2})}`;
};
const lots=(v:number|null|undefined)=>{
 const n=num(v);
 if(n==null)return DASH;
 return `${n>0?'+':n<0?MINUS:''}${compact(Math.abs(n))}`;
};
const points=(v:number|null|undefined)=>{
 const n=num(v);
 return n==null?DASH:`${Math.abs(n).toLocaleString('en-IN',{maximumFractionDigits:0})} points`;
};

/** TIER 1 HEADLINES. One per story state, and not one of them names a participant. They say what the
/** TIER 1 HEADLINES. One per story state, and not one of them names a participant: they say what the
 *  measured numbers did between two named readings, which is the only thing this engine can prove.
 *
 *  THE VERB FOLLOWS THE OPEN INTEREST. A headline reading "positions begin building" over a body reading
 *  "open interest fell" is the screen contradicting itself — the same defect the claim policy exists to
 *  stop, one level up. `x` is true for the two behaviours that are a position reduction. */
const HEADLINE:Record<StoryState,(side:string,n:number,exiting:boolean)=>string>={
 appeared:(s,_n,x)=>`${s} positions begin ${x?'reducing':'building'}`,
 continued:(s,_n,x)=>`${s} positions keep ${x?'reducing':'building'} at the same strikes`,
 strengthened:(s,_n,x)=>`${s} position ${x?'reduction':'building'} picks up`,
 slowed:(s,_n,x)=>`${s} position ${x?'reduction':'building'} slows`,
 broadened:(s,n,x)=>`${s} positions ${x?'reduce':'build'} at ${n===1?'another strike':`${n} more strikes`}`,
 narrowed:(s,n,x)=>`${s} position ${x?'reduction':'building'} narrows to ${n===1?'one strike':`${n} strikes`}`,
 shifted:(s)=>`The largest ${s.toLowerCase()} change moves to another strike`,
 unwound:(s)=>`Earlier ${s.toLowerCase()} positions are being reduced`,
 faded:(s)=>`No further ${s.toLowerCase()} position change this interval`,
 quiet:()=>'No material change at this reading',
 not_observed:()=>'This reading carried no comparable value',
 no_baseline:()=>'Not enough readings yet to compare',
};

export const CAVEAT='Position and premium changes are observations. Naming who is buying or writing is an '
 +'interpretation of them, not a measurement: every contract opened has a buyer and a seller, the underlying’s '
 +'own move affects every premium, and nothing here is a forecast or a trade instruction.';

function windowText(obs:ReadingObservation){
 return obs.previous_at?`${clock(obs.previous_at)} → ${clock(obs.at)} IST`:`${clock(obs.at)} IST`;
}
function comparedLine(obs:ReadingObservation){
 const interval=obs.previous_at
  ?`Interval ${clock(obs.previous_at)} → ${clock(obs.at)} IST.`
  :`Reading ${clock(obs.at)} IST has no reading before it.`;
 const episode=obs.from
  ?` Episode measured against ${clock(obs.from)} IST, ${obs.window_minutes} minutes back (rule ${obs.rule_version}).`
  :'';
 return interval+episode;
}
function measuredLine(obs:ReadingObservation){
 return `Measured at this reading — calls ${obs.calls.measured} of ${obs.calls.slots} contracts, `
  +`puts ${obs.puts.measured} of ${obs.puts.slots}.`;
}

/** THE ONLY PLACE A BEHAVIOUR WORD MAY BE WRITTEN. It returns '' unless the competing explanation can be
 *  stated in the same sentence, which is the whole of the Tier 2 precondition. */
function qualified(behaviour:Behaviour,side:'calls'|'puts',spot:SpotMove|null):string{
 if(DIRECTIONAL.indexOf(behaviour)<0)return '';
 const word=BEHAVIOUR_WORD[behaviour];
 if(!spot||spot.change==null)
  return `Consistent with ${side==='puts'?'put':'call'} ${word}, though the underlying’s own move over `
   +'this interval was not captured, so the premium change cannot be separated from it.';
 const dir=spot.change>0?'rose':spot.change<0?'fell':'was unchanged';
 if(spot.change===0)
  return `Consistent with ${side==='puts'?'put':'call'} ${word}; spot was unchanged over the interval, so `
   +'the premium move is not explained by the underlying.';
 return `Consistent with ${side==='puts'?'put':'call'} ${word}, though spot also ${dir} `
  +`${points(spot.change)} over the same interval, which may explain some of the premium change.`;
}

export type SpotMove={at:string|null;from:string|null;change:number|null;spot:number|null};
export type NarrateContext={
 underlying?:string;expiry?:string;ladder?:number[];
 /** The underlying's own move over the SAME interval — the competing explanation. */
 spot?:SpotMove|null;
 /** Whole-book context, each row already scoped and worded by `crossMarket`. */
 cross?:CrossRow[];
};

/** ONE reading, in the reader's words. */
export function narrate(obs:ReadingObservation|null|undefined,ctx:NarrateContext={}):Narrative|null{
 if(!obs)return null;
 const blank:Narrative={headline:'',window:'',observed:'',qualified:'',session:'',otherSide:'',context:'',
  cross:[],evidence:[],caveat:CAVEAT,cited:[],ladder:obs.ladder||[],limited:true};
 const lead=obs.calls.weight>=obs.puts.weight?obs.calls:obs.puts;
 const other=lead===obs.calls?obs.puts:obs.calls;
 const side=SIDE_WORD[lead.row];
 const rung=ctx.ladder&&ctx.ladder.length?ctx.ladder
  :ladder(obs.strikes.filter(s=>s.row===lead.row).map(s=>s.strike));
 const win=windowText(obs);

 if(lead.state==='no_baseline')return {...blank,window:win,
  headline:HEADLINE.no_baseline('',0,false),
  observed:`This session does not yet hold a reading ${SIGNAL_WINDOW_MINUTES} minutes before `
   +`${clock(obs.at)} IST, so no change can be measured at ${ctx.underlying||'this instrument'}.`,
  evidence:[comparedLine(obs)],
  context:`Reading ${clock(obs.at)} IST · no comparison available`};

 if(lead.state==='not_observed')return {...blank,window:win,
  headline:HEADLINE.not_observed('',0,false),
  observed:`The ${clock(obs.at)} IST reading was taken, but none of the ten contracts carried a value that `
   +'could be compared, so nothing is claimed for this interval.',
  session:lead.first_at
   ?`Last confirmed ${clock(lead.first_at)}–${clock(lead.last_confirmed_at||lead.first_at)} IST. `
    +'An interval nobody observed is not counted as time the earlier activity continued.':'',
  evidence:[measuredLine(obs),comparedLine(obs)],
  context:`Reading ${clock(obs.at)} IST · not observed`};

 if(lead.behaviour==='none'&&other.behaviour==='none'){
  const faded=lead.state==='faded';
  return {...blank,limited:false,window:win,
   headline:faded?HEADLINE.faded(side,0,false):HEADLINE.quiet('',0,false),
   observed:faded
    ? `No strike moved open interest and premium together between ${clock(obs.previous_at)} and `
      +`${clock(obs.at)} IST. The outstanding open interest from earlier is still standing; that is a total, `
      +'not evidence that the same positions are still held.'
    : `No strike changed materially between ${clock(obs.previous_at)} and ${clock(obs.at)} IST. The `
      +'outstanding open interest is unchanged, which is a total rather than a statement about whose '
      +'positions remain.',
   cross:ctx.cross||[],
   evidence:[measuredLine(obs),comparedLine(obs)],
   context:`${win} · little fresh change`};
 }

 // --- the story ---------------------------------------------------------------------------------------------
 const moved=lead.state==='broadened'?lead.joined.length
  :lead.state==='narrowed'?lead.strikes.length:0;
 const exiting=EXITS.indexOf(lead.behaviour)>=0;
 const headline=(HEADLINE[lead.state]||HEADLINE.continued)(side,moved,exiting);

 // TIER 1. The measured change, plus the underlying's own move over the same window.
 const where=whereText(lead.strikes,lead.side,rung);
 const rose=lead.behaviour==='buying'||lead.behaviour==='short_covering';
 const oiWord=EXITS.indexOf(lead.behaviour)>=0?'fell':'rose';
 const priceWord=rose?'rose':'fell';
 const parts=[`Open interest ${oiWord} and premium ${priceWord} ${where}, measured against the `
  +`${clock(obs.previous_at||obs.from)} IST reading.`];
 if(lead.state==='broadened'&&lead.joined.length)
  parts.push(`${lead.joined.map(s=>strikeText(s)).join(' and ')} ${lead.side} `
   +`${lead.joined.length===1?'is new':'are new'} this interval.`);
 else if(lead.state==='narrowed'&&lead.left.length)
  parts.push(`${lead.left.map(s=>strikeText(s)).join(' and ')} ${lead.side} stopped changing this interval.`);
 else if(lead.state==='shifted')
  parts.push(`${strikeText(lead.lead)} ${lead.side} now carries the largest change.`);
 const observed=parts.join(' ');

 // TIER 2. The interpretation, welded to its competing explanation.
 const qual=qualified(lead.behaviour,lead.row,ctx.spot||null);

 // TIER 1. The session, and no claim about intervals that were not checked.
 const span=minutesText(lead.elapsed_minutes);
 const first=`${side} positions began changing at ${clock(lead.first_at)} IST`;
 const leadLine=lead.lead==null?''
  :lead.lead_stable
   ? ` ${strikeText(lead.lead)} ${lead.side} has carried the largest change at every reading since.`
   : ` ${strikeText(lead.lead)} ${lead.side} carries the largest change at this reading; the leading strike `
     +'has changed during the episode.';
 const session=`${first}${span?`, ${span} ago`:''}.${leadLine}`;

 const otherSide=other.behaviour==='none'
  ? `${SIDE_WORD[other.row]} strikes did not change materially this interval.`
  : `${SIDE_WORD[other.row]} strikes separately show open interest `
    +`${EXITS.indexOf(other.behaviour)>=0?'falling':'rising'} `
    +`${whereText(other.strikes,other.side,ladder(obs.strikes.filter(s=>s.row===other.row).map(s=>s.strike)))}.`;

 const ctxParts=[`First seen ${clock(lead.first_at)} IST`];
 if(span)ctxParts.push(`${span} so far`);
 else ctxParts.push('first reading of this episode');
 if(lead.lead!=null)ctxParts.push(`${strikeText(lead.lead)} ${lead.side} largest`);

 const evidence=[
  comparedLine(obs),
  ...lead.strikes.map(s=>{
   const row=obs.strikes.find(r=>r.strike===s&&r.row===lead.row);
   return row
    // THE STORE'S OWN WORD, IN QUOTES AND ATTRIBUTED. The tiles already print this label; repeating it
    // here as our own sentence would be a bare behaviour claim, which the policy above closes. Quoted and
    // credited to the tile it comes from, it is a fact about what another surface says.
    ? `${strikeText(s)} ${lead.side}: open interest ${lots(row.oi_change)}, premium `
      +`${money(row.price_change)} over the episode window. The ΔOI tile labels this “${row.what_label}”.`
    : '';
  }).filter(Boolean),
  ctx.spot&&ctx.spot.change!=null
   ? `Spot moved ${ctx.spot.change>0?'+':MINUS}${points(ctx.spot.change)} over the same interval.`
   : 'The underlying’s own move over this interval was not captured.',
  measuredLine(obs),
  `Strike behaviour is read from the ten at-the-money contracts the ΔOI tiles draw, under rule `
   +`${obs.rule_version}.`,
 ];

 return {headline,window:win,observed,qualified:qual,session,otherSide,
  context:ctxParts.join(' · '),cross:ctx.cross||[],
  evidence,caveat:CAVEAT,cited:lead.strikes.slice(),ladder:obs.ladder||[],limited:false};
}

/** The phrases the panel reveals, in order. Sentences, never words. */
export function phrases(n:Narrative|null|undefined):string[]{
 if(!n)return [];
 return [n.observed,n.qualified,n.otherSide,n.session].filter(Boolean)
  .flatMap(text=>String(text).split(/(?<=\.)\s+/).map(s=>s.trim()).filter(Boolean));
}

// ================================================================================================================
// CROSS-MARKET CONTEXT.
//
// Whole-book figures — the put side, the put/call ratio, futures positioning. They are NOT votes on the strike
// story and they are not scored: each row states one observation, over a named window, at its own scope. A row
// that cannot be supported is not written at all rather than written as a neutral.
//
// The trap this avoids, named because the first draft fell into it: a falling put/call ratio does NOT establish
// that puts are thinning. It falls just as readily when call open interest rises and puts do not move. So the
// row says which side actually moved.
// ================================================================================================================
type Point={at?:string|null;[k:string]:unknown};
const at2=(rows:Point[]|null|undefined,upto:string|null|undefined)=>{
 const list=(rows||[]).filter(p=>p&&p.at);
 const bound=readingMinutes(upto);
 const kept=bound==null?list:list.filter(p=>{const m=readingMinutes(String(p.at));return m!=null&&m<=bound;});
 return {now:kept[kept.length-1]||null,prev:kept[kept.length-2]||null};
};

/** The underlying's own move over the interval, from any series that carries a spot per reading. */
export function spotMove(rows:Point[]|null|undefined,upto?:string|null):SpotMove|null{
 const {now,prev}=at2(rows,upto);
 const a=now?num(now.spot):null,b=prev?num(prev.spot):null;
 if(now==null)return null;
 return {at:String(now.at||''),from:prev?String(prev.at||''):null,spot:a,
  change:a==null||b==null?null:Math.round((a-b)*100)/100};
}

export function crossMarket(input:{
 puts?:SideReading|null;
 pcr?:Point[]|null;
 futures?:Point[]|null;
 upto?:string|null;
}):CrossRow[]{
 const out:CrossRow[]=[];

 if(input.puts){
  const p=input.puts;
  out.push({label:'Puts',text:p.behaviour==='none'
   ?'Little change at the put strikes this interval.'
   :`Open interest ${EXITS.indexOf(p.behaviour)>=0?'fell':'rose'} at `
    +`${p.strikes.length} put strike${p.strikes.length===1?'':'s'} this interval.`});
 }

 const pcr=at2(input.pcr,input.upto);
 if(pcr.now&&pcr.prev){
  const a=num(pcr.now.pcr_oi),b=num(pcr.prev.pcr_oi);
  const ce=num(pcr.now.total_ce_oi),cePrev=num(pcr.prev.total_ce_oi);
  const pe=num(pcr.now.total_pe_oi),pePrev=num(pcr.prev.total_pe_oi);
  if(a!=null&&b!=null){
   const move=a>b?'rose':a<b?'fell':'was unchanged';
   // WHICH SIDE ACTUALLY MOVED. Without this the ratio gets credited to the wrong leg.
   let why='';
   if(ce!=null&&cePrev!=null&&pe!=null&&pePrev!=null){
    const dCe=ce-cePrev,dPe=pe-pePrev;
    why=Math.abs(dCe)>Math.abs(dPe)
     ?` \u2014 driven by call open interest ${dCe>=0?'rising':'falling'}, not the put side.`
     :` \u2014 driven by put open interest ${dPe>=0?'rising':'falling'}.`;
   }
   out.push({label:'PCR',text:`${a.toFixed(2)}, ${move} from ${b.toFixed(2)}${why}`});
  }
 }

 const fut=at2(input.futures,input.upto);
 if(fut.now){
  const oi=num(fut.now.oi),oiPrev=fut.prev?num(fut.prev.oi):null;
  const basis=num(fut.now.basis),basisPrev=fut.prev?num(fut.prev.basis):null;
  const bits:string[]=[];
  if(oi!=null&&oiPrev!=null)bits.push(`open interest ${oi>oiPrev?'rose':oi<oiPrev?'fell':'was flat'}`);
  if(basis!=null&&basisPrev!=null)
   bits.push(`basis ${Math.abs(basis)>Math.abs(basisPrev)?'widened':Math.abs(basis)<Math.abs(basisPrev)?'narrowed':'held'}`);
  // NO CONFLICT IS DECLARED HERE. Saying "does not agree" needs the disagreeing behaviour named, and this
  // row cannot name it; it reports what futures did and leaves the reader to weigh it.
  if(bits.length)out.push({label:'Futures',text:`Front contract ${bits.join(' and ')} this interval.`});
 }
 return out;
}
