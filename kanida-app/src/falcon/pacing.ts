// Falcon scan pacing — pure presentation logic, no imports (scripts/check-falcon.cjs evaluates this file alone).
// Rules (docs/FALCON_DISCOVER_SPEC.md §2.2):
//  1. Stages are revealed one at a time, in server order.
//  2. A revealed stage is shown "running" for at least `dwell` ms (target 2.5 s) so it can be read.
//  3. The display only moves past a stage once the SERVER reports it terminal (done / unavailable / failed). So a stage is never shown done before the server says so.
//  4. Dwell shrinks when needed so the whole reveal fits the 30 s budget (floor MIN_DWELL_MS). If the server itself is slower than 30 s, the display simply waits on it — pacing never fakes completion.
export type PaceStatus='waiting'|'running'|'done'|'unavailable'|'failed';
export type PaceStage={status:PaceStatus};
export type Pace={shown:number;since:number;start:number};
export const TARGET_MS=30000,DWELL_MS=2500,MIN_DWELL_MS=450;
export const isTerminal=(s?:PaceStatus)=>s==='done'||s==='unavailable'||s==='failed';
export function initPace(now:number,start:number=now):Pace{return {shown:0,since:now,start:Math.min(start,now)};}
/** Dwell for the stage currently shown: 2.5 s, compressed so the remaining stages fit what is left of the 30 s budget. */
export function dwellFor(p:Pace,total:number,now:number){const remaining=Math.max(1,total-p.shown);const left=TARGET_MS-(now-p.start);return Math.max(MIN_DWELL_MS,Math.min(DWELL_MS,left/remaining));}
/** Advance as far as allowed at `now` (one stage per call keeps each reveal visible; call it on every tick). */
export function stepPace(p:Pace,stages:PaceStage[],now:number,skip=false):Pace{
 const total=stages.length;if(p.shown>=total)return p;
 const cur=stages[p.shown];if(!cur||!isTerminal(cur.status))return p;           // rule 3: server first
 if(cur.status==='failed')return p;                                               // a failed stage stops the reveal; the caller shows the error
 if(skip)return {...p,shown:p.shown+1,since:now};
 if(now-p.since<dwellFor(p,total,now))return p;                                  // rules 2 + 4
 return {...p,shown:p.shown+1,since:now};
}
/** Skip: jump straight past every stage the server has finished (still never past an unfinished one). */
export function skipPace(p:Pace,stages:PaceStage[],now:number):Pace{let q=p;for(let i=0;i<stages.length;i++){const n=stepPace(q,stages,now,true);if(n===q)break;q=n;}return q;}
/** Displayed status of stage i. Stages past the reveal point read "waiting" even if the server is further ahead. */
export function shownStatus(p:Pace,stages:PaceStage[],i:number):PaceStatus{
 const st=stages[i]?.status??'waiting';
 if(i<p.shown)return st;                       // only reachable once terminal on the server
 if(i===p.shown)return st==='failed'?'failed':st==='waiting'?'waiting':'running';
 return 'waiting';
}
export const paceComplete=(p:Pace,stages:PaceStage[])=>stages.length>0&&p.shown>=stages.length;
/** 0..1 bar: finished reveals plus a capped share of the running stage's dwell. Presentation only — no number is printed from it. */
export function paceProgress(p:Pace,stages:PaceStage[],now:number){const total=stages.length;if(!total)return 0;if(p.shown>=total)return 1;const running=stages[p.shown]?.status!=='waiting';const part=running?Math.min(.85,(now-p.since)/Math.max(1,dwellFor(p,total,now))):0;return Math.min(1,(p.shown+part)/total);}
