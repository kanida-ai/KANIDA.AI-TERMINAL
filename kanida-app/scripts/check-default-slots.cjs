// Pure checks for BACKLOG 2.1 on the client: the server's live default slots reach the cards, and a user's saved
// card choice still wins over them. Run: node scripts/check-default-slots.cjs (no server needed).
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),ts=require('typescript'),assert=require('node:assert/strict');
const load=(rel,req)=>{const code=ts.transpileModule(fs.readFileSync(path.join(__dirname,'..',rel),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;const ctx={exports:{},require:req,process:{env:{}}};vm.runInNewContext(code,ctx);return ctx.exports;};
const model=load('src/model.ts',n=>{if(n==='react-native')return {Platform:{OS:'web'}};throw Error(n)});
const L=load('src/discover/logic.ts',n=>{if(n==='../model')return model;throw Error(n)});
let checks=0;const ok=fn=>{fn();checks++};
// Shaped like a researched block from /api/strategies/catalog after strategies.live_default_slots: the registry's
// Falling Wedge / Rising Wedge are kept as registry_default_slot, the live leaders own default_slot.
const st=(key,order,live,slot,registry)=>({key,order,enabled:true,side:'long',found:undefined,best_low_pct:null,detections_live:live,default_slot:slot,registry_default_slot:registry});
const list=[st('ch05-falling-wedge-1d',0,24,null,'A'),st('ch06-rising-wedge-1d',1,17,null,'B'),st('ch07-channel-1h',2,47,'B',null),st('ch25-v-bottom-1h',3,186,'A',null),st('ch01-empty-1h',4,0,null,null)];
const d=L.defaultSlots(list);
ok(()=>{assert.equal(d.A,'ch25-v-bottom-1h');assert.equal(d.B,'ch07-channel-1h')});
ok(()=>{for(const k of [d.A,d.B])assert.ok(list.find(x=>x.key===k).detections_live>0,'a default card opens on active setups')});
// no saved choice -> the live default
ok(()=>{assert.equal(L.resolveSlot(undefined,d.A,list),'ch25-v-bottom-1h');assert.equal(L.resolveSlot(undefined,d.B,list),'ch07-channel-1h')});
// a saved choice wins, even when it has no active setup right now
ok(()=>{assert.equal(L.resolveSlot('ch01-empty-1h',d.A,list),'ch01-empty-1h');assert.equal(L.resolveSlot('ch05-falling-wedge-1d',d.B,list),'ch05-falling-wedge-1d')});
// a card the user cleared stays cleared; a saved strategy that no longer exists falls back to the live default
ok(()=>{assert.equal(L.resolveSlot(null,d.A,list),null);assert.equal(L.resolveSlot('removed-key',d.A,list),'ch25-v-bottom-1h')});
console.log(`check-default-slots: ${checks} checks passed`);
