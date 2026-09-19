const fs=require('node:fs');
const vm=require('node:vm');
const ts=require('typescript');
const assert=require('node:assert/strict');
const code=ts.transpileModule(fs.readFileSync(require('node:path').join(__dirname,'../src/model.ts'),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
const context={exports:{},require:name=>{if(name==='react-native')return {Platform:{OS:'web'}};throw Error(name)},process:{env:{}},URLSearchParams,AbortController,setTimeout,clearTimeout};
vm.runInNewContext(code,context);const model=context.exports;
async function main(){
 const all=await (await fetch('http://127.0.0.1:8765/api/matches?min_trades=0')).json();let checked=0;
 for(const minimum of [0,5,20])for(const [band] of model.bands){
  const f={...model.initialFilters,minimum,band};const local=model.filterMatches(all,f).map(m=>m.id).sort();
  const remote=(await (await fetch(`http://127.0.0.1:8765/api/matches?min_trades=${minimum}&return_band=${band}`)).json()).map(m=>m.id).sort();
  assert.deepEqual(Array.from(local),remote);checked++;
 }
 for(const price of [10,100,3580,100000])for(const allocation of [10000,30000,50000])for(const risk of [.1,.5,2])for(const stop of [.5,2.5,10]){
  const p=model.sizing(price,100000,allocation,risk,stop);
  assert.ok(p.notional+p.cost<=allocation+.000001);
  assert.ok(p.loss<=100000*risk/100+.000001);
  assert.ok(Number.isInteger(p.shares)&&p.shares>=0);
 }
 const narrow=model.filterMatches(all,{...model.initialFilters,timeframe:'4H',direction:'bullish'},'wedge');
 assert.ok(narrow.every(m=>m.timeframe==='4H'&&m.direction==='bullish'&&m.pattern_name.toLowerCase().includes('wedge')));
 console.log(`${checked} frontend/server return-filter comparisons and 108 sizing scenarios passed.`);
}
main().catch(e=>{console.error(e);process.exitCode=1});
