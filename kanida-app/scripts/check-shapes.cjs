const {chromium,expect}=require('@playwright/test');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),ts=require('typescript'),assert=require('node:assert/strict');
async function main(){
 const context={exports:{}};vm.runInNewContext(ts.transpileModule(fs.readFileSync(path.join(__dirname,'../src/patternGeometry.ts'),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,context);
 const raw=await(await fetch('http://127.0.0.1:8082/api/chart?symbol=MUTHOOTFIN&timeframe=1D')).json();
 const geometry=context.exports.patternGeometry(raw.matches[0],raw.bars);
 assert.ok(geometry.swings.length>=6);assert.ok(geometry.apex.index>259);assert.equal(geometry.projection.length,2);
 for(let i=0;i<geometry.swings.length;i++){
  const p=geometry.swings[i];assert.equal(p.value,raw.bars[p.index][p.kind]);assert.ok(p.index<=256);
  if(i){assert.ok(p.index>geometry.swings[i-1].index);assert.notEqual(p.kind,geometry.swings[i-1].kind)}
 }
 const browser=await chromium.launch({channel:'msedge',headless:true});
 try {for(const [name,width,height] of [['iphone',390,844],['desktop',1440,1100]]){
  const page=await browser.newPage({viewport:{width,height}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto('http://127.0.0.1:8082/',{waitUntil:'networkidle'});
  if(width<700)await page.getByRole('button',{name:'Ask / search',exact:true}).click();
  await page.getByRole('textbox',{name:'Tell your agent what to find'}).fill('MUTHOOTFIN');
  await page.getByRole('button',{name:'Find',exact:true}).click();
  await page.getByRole('button',{name:'Inspect MUTHOOTFIN 1D Symmetrical Triangle',exact:true}).click();
  const chart=page.getByRole('button',{name:'Inspect candle date and closing price',exact:true}).last();
  await page.getByText('Structure drawn. Scenarios illustrated.',{exact:true}).last().waitFor();
  await expect(chart.locator('path[stroke="#DEE3B0"]')).toHaveCount(1);
  await chart.screenshot({path:path.join(__dirname,'../qa/'+name+'-triangle-full.png')});
  await page.getByRole('button',{name:'Hide inner swings',exact:true}).last().click();
  await expect(chart.locator('path[stroke="#DEE3B0"]')).toHaveCount(0);
  await page.getByRole('button',{name:'Show full pattern',exact:true}).last().click();
  await page.getByText('Structure drawn. Scenarios illustrated.',{exact:true}).last().waitFor();
  if(width>=1050)await page.getByRole('button',{name:'See evidence',exact:true}).last().click();
  await page.getByRole('button',{name:'Short-side history',exact:true}).click();
  await page.getByText('Structure drawn. Scenarios illustrated.',{exact:true}).last().waitFor();
  await chart.screenshot({path:path.join(__dirname,'../qa/'+name+'-triangle-short.png')});
  await page.getByRole('button',{name:'Build AutoTrade plan',exact:true}).click();
  await page.getByRole('button',{name:'Adjust account & exit rules',exact:true}).click();
  await page.getByRole('textbox',{name:'Target / stop multiple · 0.5–5',exact:true}).fill('1.5');
  await page.getByText('Custom · untested',{exact:true}).waitFor();
  await page.getByRole('textbox',{name:'Maximum holding candles · 1–260',exact:true}).fill('8');
  await page.getByRole('button',{name:'Review paper plan',exact:true}).click();
  await page.getByText('8 1D candles',{exact:true}).waitFor();
  await page.getByRole('button',{name:'Adjust the plan',exact:true}).click();
  await page.getByRole('button',{name:'Restore suggested exits',exact:true}).click();
  await expect(page.getByRole('textbox',{name:'Target / stop multiple · 0.5–5',exact:true})).toHaveValue('2');
  assert.deepEqual(errors,[]);await page.close();console.log(name+' triangle structure, side selection, custom evidence reset and restore passed');
 }}finally{await browser.close()}
 console.log('Actual pivots, alternating interior swings and projected apex verified.');
}
main().catch(e=>{console.error(e);process.exitCode=1});
