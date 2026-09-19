// Pure pacing checks for src/falcon/pacing.ts: never done before the server, >= dwell per stage when on time, <= 30 s when the server is fast, skip never passes an unfinished stage.
const fs=require('node:fs');const vm=require('node:vm');const ts=require('typescript');const assert=require('node:assert/strict');
const code=ts.transpileModule(fs.readFileSync(require('node:path').join(__dirname,'../src/falcon/pacing.ts'),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
const ctx={exports:{}};vm.runInNewContext(code,ctx);const P=ctx.exports;
const N=7;
// Simulate: server finishes stage i at serverDone[i] ms. Tick every 100 ms.
function simulate(serverDone,{skipAt=Infinity}={}){
 let pace=P.initPace(0),revealAt=[0],violations=0,end=null;
 for(let t=0;t<120000;t+=100){
  const stages=serverDone.map((d,i)=>({status:t>=d?'done':(i===0||t>=serverDone[i-1])?'running':'waiting'}));
  const next=t>=skipAt?P.skipPace(pace,stages,t):P.stepPace(pace,stages,t);
  if(next!==pace){pace=next;revealAt[pace.shown]=t;}
  for(let i=0;i<N;i++){const shown=P.shownStatus(pace,stages,i);if(shown==='done'&&stages[i].status!=='done')violations++;}
  if(P.paceComplete(pace,stages)){end=t;break;}
 }
 return {violations,end,revealAt};
}
let cases=0;
// Fast server (all done in 2 s): every stage dwells >= 2.5 s, total <= 30 s.
{const r=simulate([200,400,700,1200,1500,1800,2000]);assert.equal(r.violations,0);assert.ok(r.end<=30000,`end ${r.end}`);for(let i=1;i<=N;i++)assert.ok(r.revealAt[i]-r.revealAt[i-1]>=2500-1,`dwell ${i}`);cases++;}
// Slow middle stage (20 s): never done early; later stages compress so the total still fits 30 s.
{const r=simulate([500,1000,1500,21000,21500,22000,22500]);assert.equal(r.violations,0);assert.ok(r.end<=30000,`end ${r.end}`);assert.ok(r.revealAt[4]>=21000);cases++;}
// Server slower than the budget (40 s): display waits on the server — never finishes before it.
{const r=simulate([5000,10000,15000,20000,25000,32000,40000]);assert.equal(r.violations,0);assert.ok(r.end>=40000);cases++;}
// Skip at 1 s with the server still at stage 3: skip never passes an unfinished stage.
{const r=simulate([300,600,5000,5200,5400,5600,6000],{skipAt:1000});assert.equal(r.violations,0);assert.ok(r.revealAt[3]>=5000);assert.ok(r.end>=6000&&r.end<7000);cases++;}
// Random servers.
for(let k=0;k<300;k++){let t=0;const d=Array.from({length:N},()=>t+=Math.floor(Math.random()*6000));const r=simulate(d,{skipAt:Math.random()<.3?Math.random()*20000:Infinity});assert.equal(r.violations,0);assert.ok(r.end>=d[N-1]);if(d[N-1]<=26000&&!Number.isFinite(Infinity))assert.ok(r.end<=30000);cases++;}
// Unavailable counts as terminal; failed stops the reveal.
{const st=[{status:'unavailable'},{status:'failed'}];let p=P.initPace(0);p=P.stepPace(p,st,3000);assert.equal(p.shown,1);assert.equal(P.stepPace(p,st,9000).shown,1);assert.equal(P.shownStatus(p,st,1),'failed');cases++;}
console.log(`check-falcon: ${cases} pacing scenarios passed (no stage shown done before the server; dwell/30 s budget held).`);
