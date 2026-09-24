#!/usr/bin/env node
// The workspace layout engine (src/workbench/layout.ts), checked on the cases the owner named: 1, 2, 3, 4, 6 and
// more widgets; a wide widget; a compact one; a Full one; a resize between neighbours; a narrow screen.
//   node scripts/check-workbench-layout.cjs
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),ts=require('typescript'),assert=require('node:assert/strict');
const same=(a,b)=>assert.equal(JSON.stringify(a),JSON.stringify(b));
const code=ts.transpileModule(fs.readFileSync(path.join(__dirname,'..','src/workbench/layout.ts'),'utf8'),
 {compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
const ctx={exports:{},require:n=>{throw Error('unexpected import '+n)}};vm.runInNewContext(code,ctx);const L=ctx.exports;
let fail=0,n=0;const ok=(label,fn)=>{n++;try{fn()}catch(e){fail++;console.error('FAIL',label,'\n ',e.message)}};
const W=1392,H=736;  // a 1440 x 900 laptop: grid width and the height left under the header
const items=(sizes)=>sizes.split('').map((s,i)=>({id:'w'+i,size:s}));
const shape=(rows)=>rows.map(r=>r.items.length).join('+');
const run=(sizes,o={})=>L.layout(items(sizes),{width:W,viewport:H,...o});

ok('1 widget fills the screen',()=>{const r=run('M');assert.equal(shape(r),'1');assert.equal(r[0].height,H);});
ok('2 widgets: a balanced split, one row',()=>{const r=run('MM');assert.equal(shape(r),'2');same(r[0].items.map(x=>x.weight),[2,2]);});
ok('3 widgets: one row, the third does NOT drop to a new line',()=>assert.equal(shape(run('MMM')),'3'));
ok('4 widgets: one row of 4 at 1440px',()=>assert.equal(shape(run('MMMM')),'4'));
ok('4 with a large chart: 2 + 2, not 3 + 1',()=>assert.equal(shape(run('MMLL')),'2+2'));
ok('5 widgets: larger row first',()=>assert.equal(shape(run('MMMMM')),'3+2'));
ok('6 widgets: 3 + 3, both rows on screen',()=>{const r=run('MMMMMM');assert.equal(shape(r),'3+3');
 assert.ok(r[0].height>=320&&r[0].height*2+12<=H,'both rows fit the screen');});
ok('8 widgets: 4 + 4 on screen',()=>{const r=run('MMMMMMMM');assert.equal(shape(r),'4+4');assert.ok(r[0].height*2+12<=H);});
ok('9+ widgets: rows keep one readable height and the page scrolls',()=>{const r=run('MMMMMMMMMMMM');
 assert.ok(r.length>=3);r.forEach(x=>assert.equal(x.height,440));});
ok('the Options Trader mix of 8 (three M + an L, then S S S M) stays on two rows',()=>{const r=run('MMMLSSSM');
 assert.equal(shape(r),'4+4');assert.ok(r[0].height*2+12<=H);});
ok('scrolling rows do not balloon: tall is 520, standard 440',()=>{const r=L.layout([...items('MMMMMMMMMMMM')].map((x,i)=>({...x,tall:i<4})),{width:W,viewport:H});
 assert.equal(r[0].height,520);assert.equal(r[r.length-1].height,440);});
ok('a compact widget stays compact beside a chart',()=>{const r=run('SL');assert.equal(shape(r),'2');same(r[0].items.map(x=>x.weight),[1,3]);});
ok('minimum widths force a new row instead of crushing',()=>{const r=run('LLLL');assert.equal(shape(r),'2+2');});
ok('Full takes its row; the widgets around it still balance',()=>assert.equal(shape(run('MMFMM')),'2+1+2'));
ok('a lone widget in a row takes the whole row',()=>{const r=run('F');assert.equal(r[0].items[0].weight,1);assert.equal(r[0].items[0].min,0);});
ok('the column cap is honoured',()=>assert.equal(shape(run('MMMMMM',{columns:2})),'2+2+2'));
ok('narrow screen: fewer per row',()=>assert.equal(shape(L.layout(items('MMMM'),{width:700,viewport:700})),'2+2'));
ok('phone: one ordered column',()=>{const r=L.layout(items('MLS'),{width:360,viewport:700,phone:true});assert.equal(shape(r),'1+1+1');assert.equal(r[1].height,560);});
ok('resize snaps between neighbours',()=>{same(L.snapPair(500,500),['M','M']);same(L.snapPair(750,250),['L','S']);
 same(L.snapPair(330,660),['S','M']);same(L.snapPair(600,400),['L','M']);});
ok('order is never changed by the layout',()=>{const r=run('SMLMSLM');same(r.flatMap(x=>x.items.map(i=>i.id)),['w0','w1','w2','w3','w4','w5','w6']);});
ok('a widget that needs more room than its class (an option chain) gets it, and the row adapts',()=>{
 const xs=items('MMMM');xs[1].min=420;const r=L.layout(xs,{width:W,viewport:H});
 const cell=r.flatMap(x=>x.items).find(c=>c.id==='w1');assert.equal(cell.min,420);
 const row=r.find(x=>x.items.some(c=>c.id==='w1'));assert.ok(row.items.reduce((a,c)=>a+c.min,0)+12*(row.items.length-1)<=W);});
console.log(`check-workbench-layout: ${n-fail}/${n} passed`);process.exit(fail?1:0);
