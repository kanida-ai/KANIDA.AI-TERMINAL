#!/usr/bin/env node
// The screener's state words are the Derivative tab's own rules. This runs the TypeScript ORIGINALS in
// src/derivative/logic.ts on the fixture the Python ports are tested against (server/tests/fixtures/
// screener_parity.json, written by tests/test_screener_states.py) and refuses any difference - and checks that
// the constants the Python side quotes are the ones logic.ts / summary.ts actually hold.
//
//   node scripts/check-screener.cjs
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),ts=require('typescript'),assert=require('node:assert/strict');
const root=path.join(__dirname,'..');
const load=(rel)=>{
 const code=ts.transpileModule(fs.readFileSync(path.join(root,rel),'utf8'),
  {compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
 const ctx={exports:{},require:n=>{throw Error('unexpected import: '+n)},process:{env:{}}};
 vm.runInNewContext(code,ctx);return ctx.exports;
};
const L=load('src/derivative/logic.ts');
const fixture=JSON.parse(fs.readFileSync(path.join(root,'server/tests/fixtures/screener_parity.json'),'utf8'));
const summary=fs.readFileSync(path.join(root,'src/derivative/summary.ts'),'utf8');
const pace=/export const PACE_UP=([\d.]+),PACE_DOWN=([\d.]+)/.exec(summary);
let failures=0,checks=0;
const ok=(fn,label)=>{checks++;try{fn()}catch(e){failures++;console.error('FAIL',label,'\n ',e.message)}};

ok(()=>{
 assert.equal(L.GRID_FLAT_FRACTION,fixture.constants.GRID_FLAT_FRACTION);
 assert.equal(L.GRID_PRICE_FLAT_FRACTION,fixture.constants.GRID_PRICE_FLAT_FRACTION);
 assert.equal(L.SESSION_FLAT_FRACTION,fixture.constants.SESSION_FLAT_FRACTION);
 assert.ok(pace,'PACE_UP / PACE_DOWN not found in summary.ts');
 assert.equal(Number(pace[1]),fixture.constants.PACE_UP);
 assert.equal(Number(pace[2]),fixture.constants.PACE_DOWN);
},'constants');

const oiWord={building:'up',flat:'flat',unwinding:'down','no baseline':null};
const plain={up:'up',flat:'flat',down:'down','no baseline':null};
fixture.cases.forEach((c,i)=>{
 ok(()=>{
  const oi=L.gridDirection(c.values.map(v=>({delta_oi:v})),c.lookback);
  const price=L.gridPriceDirection(c.values.map(v=>({price:v})),c.lookback);
  const session=L.sessionDirection(c.values,['up','flat','down'],c.lookback);
  assert.equal(oiWord[oi],c.grid_oi,'gridDirection');
  assert.equal(plain[price],c.grid_price,'gridPriceDirection');
  assert.equal(plain[session],c.session,'sessionDirection');
 },`case ${i} ${JSON.stringify(c.values)} lookback ${c.lookback}`);
});
console.log(`check-screener: ${checks-failures}/${checks} passed`);
process.exit(failures?1:0);
